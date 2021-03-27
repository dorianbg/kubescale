Overview
------------------

KubeScale is a hybrid auto-scaler primarily focused on horizontally scaling containerised applications deployed on Kubernetes.
It automatically scales deployments on Kubernetes by <b>predicting</b> the future workload and adjusting the resources beforehand.

The main benefit provided by KubeScale is optimisation of resource usage.

Some of the problems solved by the KubeScale auto-scaler are:
- how to determine when to scale up
- how to determine when to scale down
- how much to scale the resources
- ...

![KubeScale algorithm](KubeScale_algo.jpg)
