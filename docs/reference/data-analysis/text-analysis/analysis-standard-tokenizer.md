---
navigation_title: "Standard"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-standard-tokenizer.html
---

# Standard tokenizer [analysis-standard-tokenizer]


The `standard` tokenizer provides grammar based tokenization (based on the Unicode Text Segmentation algorithm, as specified in [Unicode Standard Annex #29](https://unicode.org/reports/tr29/)) and works well for most languages.


## Example output [_example_output_16]

```console
POST _analyze
{
  "tokenizer": "standard",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

% 
% [source,console-result]
% ----------------------------
% {
%   "tokens": [
%     {
%       "token": "The",
%       "start_offset": 0,
%       "end_offset": 3,
%       "type": "<ALPHANUM>",
%       "position": 0
%     },
%     {
%       "token": "2",
%       "start_offset": 4,
%       "end_offset": 5,
%       "type": "<NUM>",
%       "position": 1
%     },
%     {
%       "token": "QUICK",
%       "start_offset": 6,
%       "end_offset": 11,
%       "type": "<ALPHANUM>",
%       "position": 2
%     },
%     {
%       "token": "Brown",
%       "start_offset": 12,
%       "end_offset": 17,
%       "type": "<ALPHANUM>",
%       "position": 3
%     },
%     {
%       "token": "Foxes",
%       "start_offset": 18,
%       "end_offset": 23,
%       "type": "<ALPHANUM>",
%       "position": 4
%     },
%     {
%       "token": "jumped",
%       "start_offset": 24,
%       "end_offset": 30,
%       "type": "<ALPHANUM>",
%       "position": 5
%     },
%     {
%       "token": "over",
%       "start_offset": 31,
%       "end_offset": 35,
%       "type": "<ALPHANUM>",
%       "position": 6
%     },
%     {
%       "token": "the",
%       "start_offset": 36,
%       "end_offset": 39,
%       "type": "<ALPHANUM>",
%       "position": 7
%     },
%     {
%       "token": "lazy",
%       "start_offset": 40,
%       "end_offset": 44,
%       "type": "<ALPHANUM>",
%       "position": 8
%     },
%     {
%       "token": "dog’s",
%       "start_offset": 45,
%       "end_offset": 50,
%       "type": "<ALPHANUM>",
%       "position": 9
%     },
%     {
%       "token": "bone",
%       "start_offset": 51,
%       "end_offset": 55,
%       "type": "<ALPHANUM>",
%       "position": 10
%     }
%   ]
% }
% ----------------------------
% 

The above sentence would produce the following terms:

```text
[ The, 2, QUICK, Brown, Foxes, jumped, over, the, lazy, dog's, bone ]
```


## Configuration [_configuration_19]

The `standard` tokenizer accepts the following parameters:

`max_token_length`
:   The maximum token length. If a token is seen that exceeds this length then it is split at `max_token_length` intervals. Defaults to `255`.


## Example configuration [_example_configuration_13]

In this example, we configure the `standard` tokenizer to have a `max_token_length` of 5 (for demonstration purposes):

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "my_tokenizer"
        }
      },
      "tokenizer": {
        "my_tokenizer": {
          "type": "standard",
          "max_token_length": 5
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_analyzer",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

% 
% [source,console-result]
% ----------------------------
% {
%   "tokens": [
%     {
%       "token": "The",
%       "start_offset": 0,
%       "end_offset": 3,
%       "type": "<ALPHANUM>",
%       "position": 0
%     },
%     {
%       "token": "2",
%       "start_offset": 4,
%       "end_offset": 5,
%       "type": "<NUM>",
%       "position": 1
%     },
%     {
%       "token": "QUICK",
%       "start_offset": 6,
%       "end_offset": 11,
%       "type": "<ALPHANUM>",
%       "position": 2
%     },
%     {
%       "token": "Brown",
%       "start_offset": 12,
%       "end_offset": 17,
%       "type": "<ALPHANUM>",
%       "position": 3
%     },
%     {
%       "token": "Foxes",
%       "start_offset": 18,
%       "end_offset": 23,
%       "type": "<ALPHANUM>",
%       "position": 4
%     },
%     {
%       "token": "jumpe",
%       "start_offset": 24,
%       "end_offset": 29,
%       "type": "<ALPHANUM>",
%       "position": 5
%     },
%     {
%       "token": "d",
%       "start_offset": 29,
%       "end_offset": 30,
%       "type": "<ALPHANUM>",
%       "position": 6
%     },
%     {
%       "token": "over",
%       "start_offset": 31,
%       "end_offset": 35,
%       "type": "<ALPHANUM>",
%       "position": 7
%     },
%     {
%       "token": "the",
%       "start_offset": 36,
%       "end_offset": 39,
%       "type": "<ALPHANUM>",
%       "position": 8
%     },
%     {
%       "token": "lazy",
%       "start_offset": 40,
%       "end_offset": 44,
%       "type": "<ALPHANUM>",
%       "position": 9
%     },
%     {
%       "token": "dog’s",
%       "start_offset": 45,
%       "end_offset": 50,
%       "type": "<ALPHANUM>",
%       "position": 10
%     },
%     {
%       "token": "bone",
%       "start_offset": 51,
%       "end_offset": 55,
%       "type": "<ALPHANUM>",
%       "position": 11
%     }
%   ]
% }
% ----------------------------
% 

The above example produces the following terms:

```text
[ The, 2, QUICK, Brown, Foxes, jumpe, d, over, the, lazy, dog's, bone ]
```

