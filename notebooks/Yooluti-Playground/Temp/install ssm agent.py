# Databricks notebook source
1+1

# COMMAND ----------

# MAGIC %sh wget https://s3.amazonaws.com/ec2-downloads-windows/SSMAgent/latest/debian_amd64/amazon-ssm-agent.deb -O /tmp/amazon-ssm-agent.deb

# COMMAND ----------

# MAGIC %sh 
# MAGIC sudo apt install /tmp/amazon-ssm-agent.deb -y

# COMMAND ----------

