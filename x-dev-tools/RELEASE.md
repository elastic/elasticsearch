# Releasing x-plugins

Releasing is split into two steps now. First, elasticsearch core gets a testing release.
This release is not published on the maven sonatype repo, but is left as a staging repository.
At the same time, the created artifacts are uploaded to the download.elastic.co S3 bucket into
a private URL, that is comprised of the version and the commit hash.

As soon as the core is uploaded there, we can use this repository to build x-plugins against it.

## Release x-plugins for a release candidate

In order to upload x-plugins to the elasticsearch core release candidate, you need to execute the
`release_beta_to_s3` shell script. There are two parameters, first the version to be released and
second the commit hash, which is part of the URL to download this beta release.

```
sh x-dev-tools/release_beta_to_s3.sh 2.0.0-beta1 468cb61
```

So, what is this script doing

* Adds the S3 bucket as repository
* Updates the versions in the pom.xml to reflect the non-snapshot version
* runs `mvn install` into a local repository, then removes sources and emits the s3cmd command to sync with the core repo

As soon as the Elasticsearch is released, this S3 bucket is just renamed. This
means, that at this moment, the x-plugins will work for the plugin manager, 
without you having to do anthing!

However, one last step remains...

## Deploying x-plugins into maven repositories

For those, who want to use the jar files as part of their java projects, we have
to update our own artifactory after the release has been done.

In addition to those steps like updating documentation we have to run deploy
to this repo. What is required for this, is the version of the release and
the commit hash of the commit in the `x-plugins` repo. 

**NOTE: This is not the same commit hash as above** in the release candidate
process. Luckily the script should fail, when git does not find the commit!

```
sh release_to_repositories.sh 2.0.0-beta1 75964ad
```

This script checks out the specified commit hash, removes all the
`-SNAPSHOT` suffixes and runs `mvn deploy -Pdeploy-public`. The script
will also wait for you pressing enter before proceeding!

