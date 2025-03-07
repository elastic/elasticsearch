---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-gce-usage-cloning.html
---

# Cloning your existing machine [discovery-gce-usage-cloning]

In order to build a cluster on many nodes, you can clone your configured instance to new nodes. You won’t have to reinstall everything!

First create an image of your running instance and upload it to Google Cloud Storage:

```sh
# Create an image of your current instance
sudo /usr/bin/gcimagebundle -d /dev/sda -o /tmp/

# An image has been created in `/tmp` directory:
ls /tmp
e4686d7f5bf904a924ae0cfeb58d0827c6d5b966.image.tar.gz

# Upload your image to Google Cloud Storage:
# Create a bucket to hold your image, let's say `esimage`:
gsutil mb gs://esimage

# Copy your image to this bucket:
gsutil cp /tmp/e4686d7f5bf904a924ae0cfeb58d0827c6d5b966.image.tar.gz gs://esimage

# Then add your image to images collection:
gcloud compute images create elasticsearch-2-0-0 --source-uri gs://esimage/e4686d7f5bf904a924ae0cfeb58d0827c6d5b966.image.tar.gz

# If the previous command did not work for you, logout from your instance
# and launch the same command from your local machine.
```

## Start new instances [discovery-gce-usage-start-new-instances]

As you have now an image, you can create as many instances as you need:

```sh
# Just change node name (here myesnode2)
gcloud compute instances create myesnode2 --image elasticsearch-2-0-0 --zone europe-west1-a

# If you want to provide all details directly, you can use:
gcloud compute instances create myesnode2 --image=elasticsearch-2-0-0 \
       --zone europe-west1-a --machine-type f1-micro --scopes=compute-rw
```


## Remove an instance (aka shut it down) [discovery-gce-usage-remove-instance]

You can use [Google Cloud Console](https://cloud.google.com/console) or CLI to manage your instances:

```sh
# Stopping and removing instances
gcloud compute instances delete myesnode1 myesnode2 \
       --zone=europe-west1-a

# Consider removing disk as well if you don't need them anymore
gcloud compute disks delete boot-myesnode1 boot-myesnode2  \
       --zone=europe-west1-a
```


