

## Grunt build system
This grunt-based build system handles Kibana development environment setup for Marvel as well as building, packaging and distribution of the Marvel plugin. Note that you **must** run *grunt setup* before any other tasks as this build system reuses parts of the Kibana build system that must be fetched

### Installing
You will need node.js+npm and grunt. Node is available via brew, install grunt with the command below. Once grunt is installed you may run grunt tasks to setup your environment and build Marvel

```npm install -g grunt```
```npm install```

### Tasks

```grunt setup```

**Run this first.** It will download the right Kibana version to ./vendor/kibana, copies the appropriate config.js to the right place and make any symlinks needed for a proper marvel/kibana environment

```grunt server```

Starts a web server on http://127.0.0.1:5601 pointing at the kibana directory, while also serving custom marvel panels.

You can use `grunt server --port=5601 --es_host=9200` to control the ports used for kibana and the elasticsearch port used.

```grunt jshint```

Lints code without building

```grunt build```

Merges kibana and marvel code, builds Kibana and the plugin (via mvn) and puts them in ./build.

```grunt package```

Zips and tar+gzips the build in ./packages. Includes grunt build

```grunt release```

Uploads created archives to download.elasticsearch.org/elasticsearch/marvel/marvel-VERSION.extention. You will need S3 credentials in .aws-config.json. Format as so:

```
{
  "key":"MY_KEY_HERE",
  "secret":"your/long/secret/string"
}

```