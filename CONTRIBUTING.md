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
  - Start Elasticsearch, with esvm `esvm --branch 2.1` or with traditionally `bin/elasticsearch`.
  - Start up the mock Marvel agent with `gulp index`
  - Start up the filesystem watching of Marvel code and syncing to Kibana's plugin directory with `gulp dev`
  - Finally startup Kibana from that directory run `bin/kibana`

- Check it out, navigate to your [Kibana App](http://localhost:5601)
