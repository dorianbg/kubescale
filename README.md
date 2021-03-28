KubeScale
------------------

KubeScale is a hybrid auto-scaler primarily focused on horizontally scaling containerised applications deployed on Kubernetes.
It automatically scales deployments on Kubernetes by <b>predicting</b> the future workload and adjusting the resources beforehand.
The workload prediction is done using a Deep auto-regressive estimator 

Some of the problems solved by the KubeScale auto-scaler are:
- how to determine when to scale up
- how to determine when to scale down
- how much to scale the resources
- ...


Existing metrics sources are:
- Requests per second
- CPU usage

To integrate a new source of metrics use the simple MetricSource interface.
You should choose metrics that are likely to present the overall workload on the application.


The high level algorithm behind KubeScale is the following: 
![KubeScale algorithm](KubeScale_algo.jpg)

An example of an alert the KubeScale auto-scaler sends:
![KubeScale email alert](KubeScale_email_alert.jpg)
