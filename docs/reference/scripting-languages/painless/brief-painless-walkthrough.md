---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-walkthrough.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

:::{tip}
This walkthrough is designed for users experienced with scripting and already familiar with Painless, who need direct access to Painless specifications and advanced features.

<!--
If you're new to Painless scripting or need step-by-step guidance, start with [How to write scripts](docs-content://explore-analyze/scripting/modules-scripting-using) in the Explore and Analyze section for a more accessible introduction.
-->

:::

# A brief Painless walkthrough [painless-walkthrough]

To illustrate how Painless works, let’s load some hockey stats into an {{es}} index:

```console
PUT hockey/_bulk?refresh
{"index":{"_id":1}}
{"first":"johnny","last":"gaudreau","goals":[9,27,1],"assists":[17,46,0],"gp":[26,82,1],"born":"1993/08/13"}
{"index":{"_id":2}}
{"first":"sean","last":"monohan","goals":[7,54,26],"assists":[11,26,13],"gp":[26,82,82],"born":"1994/10/12"}
{"index":{"_id":3}}
{"first":"jiri","last":"hudler","goals":[5,34,36],"assists":[11,62,42],"gp":[24,80,79],"born":"1984/01/04"}
{"index":{"_id":4}}
{"first":"micheal","last":"frolik","goals":[4,6,15],"assists":[8,23,15],"gp":[26,82,82],"born":"1988/02/17"}
{"index":{"_id":5}}
{"first":"sam","last":"bennett","goals":[5,0,0],"assists":[8,1,0],"gp":[26,1,0],"born":"1996/06/20"}
{"index":{"_id":6}}
{"first":"dennis","last":"wideman","goals":[0,26,15],"assists":[11,30,24],"gp":[26,81,82],"born":"1983/03/20"}
{"index":{"_id":7}}
{"first":"david","last":"jones","goals":[7,19,5],"assists":[3,17,4],"gp":[26,45,34],"born":"1984/08/10"}
{"index":{"_id":8}}
{"first":"tj","last":"brodie","goals":[2,14,7],"assists":[8,42,30],"gp":[26,82,82],"born":"1990/06/07"}
{"index":{"_id":39}}
{"first":"mark","last":"giordano","goals":[6,30,15],"assists":[3,30,24],"gp":[26,60,63],"born":"1983/10/03"}
{"index":{"_id":10}}
{"first":"mikael","last":"backlund","goals":[3,15,13],"assists":[6,24,18],"gp":[26,82,82],"born":"1989/03/17"}
{"index":{"_id":11}}
{"first":"joe","last":"colborne","goals":[3,18,13],"assists":[6,20,24],"gp":[26,67,82],"born":"1990/01/30"}
```
% TESTSETUP

With the hockey data ingested, try out some basic operations that you can do in Painless:

- [](/reference/scripting-languages/painless/painless-walkthrough-access-doc-values.md)
- [](/reference/scripting-languages/painless/painless-walkthrough-missing-keys-or-values.md)
- [](/reference/scripting-languages/painless/painless-walkthrough-updating-fields.md)
- [](/reference/scripting-languages/painless/painless-walkthrough-dates.md)
- [](/reference/scripting-languages/painless/painless-walkthrough-regular-expressions.md)
