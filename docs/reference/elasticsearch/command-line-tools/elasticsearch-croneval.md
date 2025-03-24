---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/elasticsearch-croneval.html
---

# elasticsearch-croneval [elasticsearch-croneval]

Validates and evaluates a [cron expression](/reference/elasticsearch/rest-apis/api-conventions.md#api-cron-expressions).


## Synopsis [_synopsis_4]

```shell
bin/elasticsearch-croneval <expression>
[-c, --count <integer>] [-h, --help]
([-s, --silent] | [-v, --verbose])
```


## Description [_description_11]

This command enables you to verify that your cron expressions are valid for use with {{es}} and produce the expected results.

This command is provided in the `$ES_HOME/bin` directory.


## Parameters [elasticsearch-croneval-parameters]

`-c, --count` <Integer>
:   The number of future times this expression will be triggered. The default value is `10`.

`-d, --detail`
:   Shows detail for invalid cron expression. It will print the stacktrace if the expression is not valid.

`-h, --help`
:   Returns all of the command parameters.

`-s, --silent`
:   Shows minimal output.

`-v, --verbose`
:   Shows verbose output.


## Example [_example_11]

If the cron expression is valid, the following command displays the next 20 times that the schedule will be triggered:

```bash
bin/elasticsearch-croneval "0 0/1 * * * ?" -c 20
```

