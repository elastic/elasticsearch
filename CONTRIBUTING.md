# Development Environment Setup

- You must have first setup [Kibana](https://github.com/elastic/kibana) by following their [contributing](https://github.com/elastic/kibana/blob/master/CONTRIBUTING.md) docs.

- Next esvm is strongly suggested simply `npm install -g esvm`

- Clone and go into this project
```
git clone https://github.com/elastic/elasticsearch-marvel.git && cd elasticsearch-marvel
```
- install dependencies
```
npm install
```
- Start it up
  - Start Elasticsearch, with esvm `esvm --branch 2.0` or with traditionally `bin/elasticsearch`.
  - Start up the mock Marvel agent with `gulp index`
  - Start up the watching and sync of marvel and Kibana with `gulp dev`
  - Finally startup Kibana from that directory run `bin/kibana`

- Check it out, navigate to your [Kibana App](http://localhost:5601)
