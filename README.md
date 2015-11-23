# WARNING THIS IS ALL IN FLUX


# Testing/Installing Marvel 2.1.0

The easiest way to get to know the new Marvel is probably by [reading the docs](https://github.com/elastic/elasticsearch-marvel/blob/master/docs/index.asciidoc). 
The second easiest way is to just install it.

- Install the marvel plugin on kibana `./bin/kibana plugin -i elasticsearch/marvel/2.1.0`
- Install the License plugin on your cluster `./bin/plugin install license`
- Install the Marvel agent on your cluster `./bin/plugin install marvel-agent`

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

