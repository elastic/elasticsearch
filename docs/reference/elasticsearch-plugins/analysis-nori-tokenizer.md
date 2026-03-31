---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-nori-tokenizer.html
---

# nori_tokenizer [analysis-nori-tokenizer]

The `nori_tokenizer` accepts the following settings:

`decompound_mode`
:   The decompound mode determines how the tokenizer handles compound tokens. It can be set to:

`none`
:   No decomposition for compounds. Example output:

    ```
    가거도항
    가곡역
    ```


`discard`
:   Decomposes compounds and discards the original form (**default**). Example output:

    ```
    가곡역 => 가곡, 역
    ```


`mixed`
:   Decomposes compounds and keeps the original form. Example output:

    ```
    가곡역 => 가곡역, 가곡, 역
    ```



`discard_punctuation`
:   Whether punctuation should be discarded from the output. Defaults to `true`.

`lenient`
:   Whether the `user_dictionary` should be deduplicated on the provided `text`. False by default causing duplicates to generate an error.

`user_dictionary`
:   The Nori tokenizer uses the [mecab-ko-dic dictionary](https://bitbucket.org/eunjeon/mecab-ko-dic) by default. A `user_dictionary` with custom nouns (`NNG`) may be appended to the default dictionary. The dictionary should have the following format:

```txt
<token> [<token 1> ... <token n>]
```

The first token is mandatory and represents the custom noun that should be added in the dictionary. For compound nouns the custom segmentation can be provided after the first token (`[<token 1> ... <token n>]`). The segmentation of the custom compound nouns is controlled by the `decompound_mode` setting.

As a demonstration of how the user dictionary can be used, save the following dictionary to `$ES_HOME/config/userdict_ko.txt`:

```txt
c++                 <1>
C쁠쁠
세종
세종시 세종 시        <2>
```

1. A simple noun
2. A compound noun (`세종시`) followed by its decomposition: `세종` and `시`.


Then create an analyzer as follows:

```console
PUT nori_sample
{
  "settings": {
    "index": {
      "analysis": {
        "tokenizer": {
          "nori_user_dict": {
            "type": "nori_tokenizer",
            "decompound_mode": "mixed",
            "discard_punctuation": "false",
            "user_dictionary": "userdict_ko.txt",
            "lenient": "true"
          }
        },
        "analyzer": {
          "my_analyzer": {
            "type": "custom",
            "tokenizer": "nori_user_dict"
          }
        }
      }
    }
  }
}

GET nori_sample/_analyze
{
  "analyzer": "my_analyzer",
  "text": "세종시"  <1>
}
```

1. Sejong city


The above `analyze` request returns the following:

```console-result
{
  "tokens" : [ {
    "token" : "세종시",
    "start_offset" : 0,
    "end_offset" : 3,
    "type" : "word",
    "position" : 0,
    "positionLength" : 2    <1>
  }, {
    "token" : "세종",
    "start_offset" : 0,
    "end_offset" : 2,
    "type" : "word",
    "position" : 0
  }, {
    "token" : "시",
    "start_offset" : 2,
    "end_offset" : 3,
    "type" : "word",
    "position" : 1
   }]
}
```

1. This is a compound token that spans two positions (`mixed` mode).



`user_dictionary_rules`
:   You can also inline the rules directly in the tokenizer definition using the `user_dictionary_rules` option:

```console
PUT nori_sample
{
  "settings": {
    "index": {
      "analysis": {
        "tokenizer": {
          "nori_user_dict": {
            "type": "nori_tokenizer",
            "decompound_mode": "mixed",
            "user_dictionary_rules": ["c++", "C쁠쁠", "세종", "세종시 세종 시"]
          }
        },
        "analyzer": {
          "my_analyzer": {
            "type": "custom",
            "tokenizer": "nori_user_dict"
          }
        }
      }
    }
  }
}
```


The `nori_tokenizer` sets a number of additional attributes per token that are used by token filters to modify the stream. You can view all these additional attributes with the following request:

```console
GET _analyze
{
  "tokenizer": "nori_tokenizer",
  "text": "뿌리가 깊은 나무는",   <1>
  "attributes" : ["posType", "leftPOS", "rightPOS", "morphemes", "reading"],
  "explain": true
}
```

1. A tree with deep roots


Which responds with:

```console-result
{
  "detail": {
    "custom_analyzer": true,
    "charfilters": [],
    "tokenizer": {
      "name": "nori_tokenizer",
      "tokens": [
        {
          "token": "뿌리",
          "start_offset": 0,
          "end_offset": 2,
          "type": "word",
          "position": 0,
          "leftPOS": "NNG(General Noun)",
          "morphemes": null,
          "posType": "MORPHEME",
          "reading": null,
          "rightPOS": "NNG(General Noun)"
        },
        {
          "token": "가",
          "start_offset": 2,
          "end_offset": 3,
          "type": "word",
          "position": 1,
          "leftPOS": "JKS(Subject case marker)",
          "morphemes": null,
          "posType": "MORPHEME",
          "reading": null,
          "rightPOS": "JKS(Subject case marker)"
        },
        {
          "token": "깊",
          "start_offset": 4,
          "end_offset": 5,
          "type": "word",
          "position": 2,
          "leftPOS": "VA(Adjective)",
          "morphemes": null,
          "posType": "MORPHEME",
          "reading": null,
          "rightPOS": "VA(Adjective)"
        },
        {
          "token": "은",
          "start_offset": 5,
          "end_offset": 6,
          "type": "word",
          "position": 3,
          "leftPOS": "ETM(Adnominal form transformative ending)",
          "morphemes": null,
          "posType": "MORPHEME",
          "reading": null,
          "rightPOS": "ETM(Adnominal form transformative ending)"
        },
        {
          "token": "나무",
          "start_offset": 7,
          "end_offset": 9,
          "type": "word",
          "position": 4,
          "leftPOS": "NNG(General Noun)",
          "morphemes": null,
          "posType": "MORPHEME",
          "reading": null,
          "rightPOS": "NNG(General Noun)"
        },
        {
          "token": "는",
          "start_offset": 9,
          "end_offset": 10,
          "type": "word",
          "position": 5,
          "leftPOS": "JX(Auxiliary postpositional particle)",
          "morphemes": null,
          "posType": "MORPHEME",
          "reading": null,
          "rightPOS": "JX(Auxiliary postpositional particle)"
        }
      ]
    },
    "tokenfilters": []
  }
}
```

