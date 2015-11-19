# WARNING THIS IS ALL IN FLUX

<<<<<<< HEAD
# Testing/Installing Marvel 2.0.0
=======
<<<<<<< HEAD
>>>>>>> 46ac02d... Docs: Bumped version number to 2.1.0 and added 2.1.0 release notes.

The easiest way to get to know the new Marvel is probably by [reading the docs](https://github.com/elastic/elasticsearch-marvel/blob/master/docs/index.asciidoc), the second easiest way, however, is probably to install the 2.0.0 release version of it.

<<<<<<< HEAD
- Install the marvel plugin on kibana ` ./bin/kibana plugin -i elasticsearch/marvel/2.0.0 `
- Install the License plugin on your cluster `./bin/plugin install license`
- Install the Marvel agent on your cluster ` ./bin/plugin install marvel-agent `
=======
The easiest way to play/get to know Marvel is to install the latest release version of it. To do so, just run the following command on every node on your cluster (restart node for it to have effect):
=======
# Testing/Installing Marvel 2.1.0

The easiest way to get to know the new Marvel is probably by [reading the docs](https://github.com/elastic/elasticsearch-marvel/blob/master/docs/index.asciidoc), the second easiest way, however, is probably to install the 2.1.0 release version of it.

- Install the marvel plugin on kibana `./bin/kibana plugin -i elasticsearch/marvel/2.1.0`
- Install the License plugin on your cluster `./bin/plugin install license`
- Install the Marvel agent on your cluster `./bin/plugin install marvel-agent`
>>>>>>> 44fbb83... Docs: Bumped version number to 2.1.0 and added 2.1.0 release notes.
>>>>>>> 46ac02d... Docs: Bumped version number to 2.1.0 and added 2.1.0 release notes.


Once done, open up the following url (assuming standard kibana config): [http://localhost:5601/app/marvel.](http://localhost:5601/app/marvel)

# Deploying Marvel
The `release task` creates archives and uploads them to download.elasticsearch.org/elasticsearch/marvel/VERSION. You will need S3 credentials in .aws-config.json. Format as so:

```
{
  "key":"MY_KEY_HERE",
  "secret":"your/long/secret/string"
}
```

To upload the current archive as the "latest" release, use:

`gulp release`

