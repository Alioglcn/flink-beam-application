import json
import argparse
import logging
import typing
import json
import typing

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import kafka
from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.transforms.external import JavaJarExpansionService


def get_expansion_service(
    jar="/opt/apache/beam/jars/beam-sdks-java-io-expansion-service.jar", args=None
):
    if args is None:
        args = [
            "--defaultEnvironmentType=PROCESS",
            '--defaultEnvironmentConfig={"command": "/opt/apache/beam/boot"}',
            "--experiments=use_deprecated_read",
        ]
    return JavaJarExpansionService(jar, ["{{PORT}}"] + args)


class NumberAccum(typing.NamedTuple):
    total: float
    count: int


beam.coders.registry.register_coder(NumberAccum, beam.coders.RowCoder)


def decode_message(kafka_kv: tuple, verbose: bool = False):
    if verbose:
        print(kafka_kv)
    return float(kafka_kv[1].decode("utf-8"))  




def create_message(element: typing.Tuple[str, str, float]):
    if element[2] > 20:
        alert_msg = "ACİL DURUM HAVA 20 DERECEYİ GEÇTİ"
    else:  
        alert_msg = "Normal"

    data = {
        "window_start": element[0],
        "window_end": element[1],
        "avg_value": element[2],
        "status": alert_msg
    }
    msg = json.dumps(data)
    print(msg)
    return "".encode("utf-8"), msg.encode("utf-8")



class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        return NumberAccum(total=0.0, count=0)

    def add_input(self, mutable_accumulator: NumberAccum, element: float):
        total, count = tuple(mutable_accumulator)
        return NumberAccum(total=total + element, count=count + 1)

    def merge_accumulators(self, accumulators: typing.List[NumberAccum]):
        totals, counts = zip(*accumulators)
        return NumberAccum(total=sum(totals), count=sum(counts))

    def extract_output(self, accumulator: NumberAccum):
        total, count = tuple(accumulator)
        return total / count if count else float("NaN")

    def get_accumulator_coder(self):
        return beam.coders.registry.get_coder(NumberAccum)


class AddWindowTS(beam.DoFn):
    def process(self, avg_value: float, win_param=beam.DoFn.WindowParam):
        yield (
            win_param.start.to_rfc3339(),
            win_param.end.to_rfc3339(),
            avg_value,
        )


class ReadNumbersFromKafka(beam.PTransform):
    def __init__(
        self,
        bootstrap_servers: str,
        topics: typing.List[str],
        group_id: str,
        verbose: bool = False,
        expansion_service: typing.Any = None,
        label: str | None = None,
    ) -> None:
        super().__init__(label)
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.verbose = verbose
        self.expansion_service = expansion_service

    def expand(self, input: pvalue.PBegin):
        return (
            input
            | "ReadFromKafka"
            >> kafka.ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": self.bootstrap_servers,
                    "auto.offset.reset": "latest",
                    "group.id": self.group_id,
                },
                topics=self.topics,
                timestamp_policy=kafka.ReadFromKafka.create_time_policy,
                commit_offset_in_finalize=True,
                expansion_service=self.expansion_service,
            )
            | "DecodeMessage" >> beam.Map(decode_message)
        )


class CalculateAvgNumber(beam.PTransform):
    def expand(self, input: pvalue.PCollection):
        return (
            input
            | "Windowing" >> beam.WindowInto(beam.window.SlidingWindows(size=3600, period=60))
            | "ComputeAverage" >> beam.CombineGlobally(AverageFn()).without_defaults()
        )



class WriteAvgToKafka(beam.PTransform):
    def __init__(
        self,
        bootstrap_servers: str,
        high_topic: str,
        low_topic: str,
        expansion_service: typing.Any = None,
        label: str | None = None,
    ) -> None:
        super().__init__(label)
        self.bootstrap_servers = bootstrap_servers
        self.high_topic = high_topic
        self.low_topic = low_topic
        self.expansion_service = expansion_service

    def expand(self, input: pvalue.PCollection):
        messages = (
            input
            | "AddWindowTS" >> beam.ParDo(AddWindowTS())
            | "CreateMessages" >> beam.Map(create_message).with_output_types(typing.Tuple[bytes, bytes])
        )

        high_temp = messages | "FilterHighTemp" >> beam.Filter(lambda x: json.loads(x[1].decode("utf-8"))["avg_value"] > 20)
        low_temp = messages | "FilterLowTemp" >> beam.Filter(lambda x: json.loads(x[1].decode("utf-8"))["avg_value"] <= 20)

        high_temp | "WriteHighTempToKafka" >> kafka.WriteToKafka(
            producer_config={"bootstrap.servers": self.bootstrap_servers},
            topic=self.high_topic,
            expansion_service=self.expansion_service,
        )

        low_temp | "WriteLowTempToKafka" >> kafka.WriteToKafka(
            producer_config={"bootstrap.servers": self.bootstrap_servers},
            topic=self.low_topic,
            expansion_service=self.expansion_service,
        )

        return messages

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--deploy",
        dest="deploy",
        action="store_true",
        default="Flag to indicate whether to deploy to a cluster",
    )
    parser.add_argument(
        "--bootstrap_servers",
        dest="bootstrap",
        default="host.docker.internal:29092",
        help="Kafka bootstrap server addresses",
    )
    parser.add_argument(
        "--input_topic",
        dest="input",
        default="input-topic",
        help="Kafka input topic name",
    )
    parser.add_argument(
        "--high_temp_topic",
        dest="high_output",
        default="output-topic-high-temp",
        help="Kafka output topic for high temperatures",
    )
    parser.add_argument(
        "--low_temp_topic",
        dest="low_output",
        default="output-topic-low-temp",
        help="Kafka output topic for low temperatures",
    )
    parser.add_argument(
        "--group_id",
        dest="group",
        default="beam-number-processing",
        help="Kafka output group ID",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    expansion_service = None
    if known_args.deploy is True:
        expansion_service = get_expansion_service()

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadNumbersFromKafka"
            >> ReadNumbersFromKafka(
                bootstrap_servers=known_args.bootstrap,
                topics=[known_args.input],
                group_id=known_args.group,
                expansion_service=expansion_service,
            )
            | "CalculateAvgNumber" >> CalculateAvgNumber()
            | "WriteAvgToKafka"
            >> WriteAvgToKafka(
                bootstrap_servers=known_args.bootstrap,
                high_topic=known_args.high_output,
                low_topic=known_args.low_output,
                expansion_service=expansion_service,
            )
        )

        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("Building pipeline ...")

if __name__ == "__main__":
    run()
