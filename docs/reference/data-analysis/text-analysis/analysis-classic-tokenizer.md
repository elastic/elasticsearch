---
navigation_title: "Classic"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-classic-tokenizer.html
---

# Classic tokenizer [analysis-classic-tokenizer]


The `classic` tokenizer is a grammar based tokenizer that is good for English language documents. This tokenizer has heuristics for special treatment of acronyms, company names, email addresses, and internet host names. However, these rules don’t always work, and the tokenizer doesn’t work well for most languages other than English:

* It splits words at most punctuation characters, removing punctuation. However, a dot that’s not followed by whitespace is considered part of a token.
* It splits words at hyphens, unless there’s a number in the token, in which case the whole token is interpreted as a product number and is not split.
* It recognizes email addresses and internet hostnames as one token.


## Example output [_example_output_8]

```console
POST _analyze
{
  "tokenizer": "classic",
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
%       "type": "<ALPHANUM>",
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
%       "type": "<APOSTROPHE>",
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


## Configuration [_configuration_9]

The `classic` tokenizer accepts the following parameters:

`max_token_length`
:   The maximum token length. If a token is seen that exceeds this length then it is split at `max_token_length` intervals. Defaults to `255`.


## Example configuration [_example_configuration_6]

In this example, we configure the `classic` tokenizer to have a `max_token_length` of 5 (for demonstration purposes):

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
          "type": "classic",
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
%       "type": "<ALPHANUM>",
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
%       "type": "<APOSTROPHE>",
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

The above example produces the following terms:

```text
[ The, 2, QUICK, Brown, Foxes, jumpe, d, over, the, lazy, dog's, bone ]
```

