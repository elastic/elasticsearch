---
navigation_title: "Character group"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-chargroup-tokenizer.html
---

# Character group tokenizer [analysis-chargroup-tokenizer]


The `char_group` tokenizer breaks text into terms whenever it encounters a character which is in a defined set. It is mostly useful for cases where a simple custom tokenization is desired, and the overhead of use of the [`pattern` tokenizer](/reference/text-analysis/analysis-pattern-tokenizer.md) is not acceptable.


## Configuration [_configuration_8]

The `char_group` tokenizer accepts one parameter:

`tokenize_on_chars`
:   A list containing a list of characters to tokenize the string on. Whenever a character from this list is encountered, a new token is started. This accepts either single characters like e.g. `-`, or character groups: `whitespace`, `letter`, `digit`, `punctuation`, `symbol`.

`max_token_length`
:   The maximum token length. If a token is seen that exceeds this length then it is split at `max_token_length` intervals. Defaults to `255`.


## Example output [_example_output_7]

```console
POST _analyze
{
  "tokenizer": {
    "type": "char_group",
    "tokenize_on_chars": [
      "whitespace",
      "-",
      "\n"
    ]
  },
  "text": "The QUICK brown-fox"
}
```

returns

```console-result
{
  "tokens": [
    {
      "token": "The",
      "start_offset": 0,
      "end_offset": 3,
      "type": "word",
      "position": 0
    },
    {
      "token": "QUICK",
      "start_offset": 4,
      "end_offset": 9,
      "type": "word",
      "position": 1
    },
    {
      "token": "brown",
      "start_offset": 10,
      "end_offset": 15,
      "type": "word",
      "position": 2
    },
    {
      "token": "fox",
      "start_offset": 16,
      "end_offset": 19,
      "type": "word",
      "position": 3
    }
  ]
}
```

