# Connect to Dataproc Serverless from VSCode Jupyter Notebook

- Set up `gcloud config`. Make sure the property `compute/region`, `core/account` and `core/project` are all set.

- Install JupyterLab

  ```sh
  pip install --upgrade jupyterlab
  ```

- Install Dataproc JupyterLab plugin

  ```sh
  pip install dataproc-jupyter-plugin
  ```

  - If your JupyterLab version is earlier than `4.0.0`, enable the plugin extension

    ```sh
    jupyter server extension enable dataproc_jupyter_plugin
    ```

- Start jupyter lab

  ```sh
  jupyter lab
  ```

- Copy server URL, make sure the token is also copied. Example:

  ```sh
  http://localhost:8888/lab?token=b5df6ca0809835e02e0d9d521b5e2c78a9429f316482a95c
  ```

- In the web browser interface of Jupyter Lab, click on `New Runtime Template` under Dataproc Serverless Notebooks segment.

- Fill the runtime template form with necessary config.

  - **Network Configuration**: The **subnetwork** must have **Private Google Access** enabled and must allow subnet communication on all ports (see [Dataproc Serverless for Spark network configuration](https://cloud.google.com/dataproc-serverless/docs/concepts/network))

  - You can view runtime template settings from **Settings > Dataproc Settings**

- In VSCode, click on the kernel top right, select `Select Another Kernel`, then click on `Existing Jupyter Server`, finally click on `Enter the URL of the running Jupyter Server`. Paste the URL obtained from above step.

- Finally, change the kernel once VSCode connect to the remote jupyter server.
