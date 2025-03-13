minikube start --cpus='max' --memory=7837 --addons=metrics-server --kubernetes-version=v1.25.3

STRIMZI_VERSION="0.39.0"
DOWNLOAD_URL=https://github.com/strimzi/strimzi-kafka-operator/releases/download/$STRIMZI_VERSION/strimzi-cluster-operator-$STRIMZI_VERSION.yaml
curl -L -o kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml ${DOWNLOAD_URL}

sed -i '' 's/namespace: .*/namespace: default/' kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml
kubectl create -f kafka/manifests/strimzi-cluster-operator-$STRIMZI_VERSION.yaml

kubectl create -f kafka/manifests/kafka-cluster.yaml
kubectl create -f kafka/manifests/kafka-ui.yaml

eval $(minikube docker-env)
docker build -t kafka-producer:latest .
kubectl apply -f kafka-producer.yaml
kubectl get pods

docker build -t beam-python-example:1.16 beam/
docker build -t beam-python-harness:2.56.0 -f beam/Dockerfile-python-harness beam/

kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator


kubectl create -f beam/temperature_cluster.yml
kubectl create -f beam/temperature_job.yml

kubectl port-forward svc/temperature-cluster-rest 8081
kubectl port-forward svc/kafka-ui 8080
#kubectl port-forward svc/demo-cluster-kafka-external-bootstrap 29092

