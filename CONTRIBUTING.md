# Development Environment Setup

- You must have first setup [Kibana](https://github.com/elastic/kibana) by following their [contributing](https://github.com/elastic/kibana/blob/master/CONTRIBUTING.md) docs.

- Next esvm is strongly suggested simply `npm install -g esvm`

- Clone and go into this project
```
git clone https://github.com/elastic/elasticsearch-marvel.git && cd elasticsearch-marvel
```
- Install dependencies
```
npm install
```
- Start it up
  - Start Elasticsearch, with esvm `esvm --branch <target version>` or traditionally with `bin/elasticsearch`.
  - Start up the mock Marvel agent with `gulp index`.
  - Start up the filesystem watching of Marvel code and syncing to Kibana's plugin directory with `gulp dev`.
  - Finally, startup Kibana by running `bin/kibana --dev` from the root of the Kibana project.
- Check it out, navigate to your [Kibana App](http://localhost:5601)

- Run tests
```
gulp test
```

- Debug tests
Add a `debugger` line to create a breakpoint, and then:
```
gulp sync && mocha debug --compilers js:babel/register /pathto/kibana/installedPlugins/marvel/pathto/__test__/testfile.js
```
