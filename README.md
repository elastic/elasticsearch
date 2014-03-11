# Installing the last release of Marvel

The easiest way to play/get to know Marvel is to install the latest release version of it. To do so, just run the following command on every node on your cluster (restart node for it to have effect):

```
./bin/plugin -i elasticsearch/marvel/latest
```

Once done, open up the following url (assuming standard ES config): http://localhost:9200/_plugin/marvel  . This will take you to the Overview Dashboard. Use Kibana's Load dashboard menu to navigate to the Cluster Pulse dashboard


## I just want to run Sense

To run Sense, checkout a copy of this repo. You can now double click `sense/index.html` and it will open in your default browser.

For a cleaner experience, we recommend using something like http://anvilformac.com and point it at the repo checkout. Once done you can access Sense via `http://webserver/sense/`, for example http://elasticsearch-marvel.dev/sense if using Anvil.

Note: to run the Kibana side of Marvel, you'd need to install grunt as described bellow.

## Grunt build system (for running the UI from a code checkout)
This grunt-based build system handles Kibana development environment setup for Marvel as well as building, packaging and distribution of the Marvel plugin. Note that you **must** run *grunt setup* before any other tasks as this build system reuses parts of the Kibana build system that must be fetched

### Installing
You will need node.js+npm and grunt. Node is available via brew, install grunt with the command below. Once grunt is installed you may run grunt tasks to setup your environment and build Marvel

```npm install -g grunt-cli```

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

Zips and tar+gzips the build in build/packages. Includes grunt build

```grunt release```

Uploads created archives to download.elasticsearch.org/elasticsearch/marvel/marvel-VERSION.extention. You will need S3 credentials in .aws-config.json. Format as so:

```
{
  "key":"MY_KEY_HERE",
  "secret":"your/long/secret/string"
}

```

To upload the current archive as the "latest" release, use:

```grunt release --latest```
