---
navigation_title: "Thai"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-thai-tokenizer.html
---

# Thai tokenizer [analysis-thai-tokenizer]


The `thai` tokenizer segments Thai text into words, using the Thai segmentation algorithm included with Java. Text in other languages in general will be treated the same as the [`standard` tokenizer](/reference/data-analysis/text-analysis/analysis-standard-tokenizer.md).

::::{warning}
This tokenizer may not be supported by all JREs. It is known to work with Sun/Oracle and OpenJDK. If your application needs to be fully portable, consider using the [ICU Tokenizer](/reference/elasticsearch-plugins/analysis-icu-tokenizer.md) instead.
::::



## Example output [_example_output_17]

```console
POST _analyze
{
  "tokenizer": "thai",
  "text": "การที่ได้ต้องแสดงว่างานดี"
}
```

% 
% [source,console-result]
% ----------------------------
% {
%   "tokens": [
%     {
%       "token": "การ",
%       "start_offset": 0,
%       "end_offset": 3,
%       "type": "word",
%       "position": 0
%     },
%     {
%       "token": "ที่",
%       "start_offset": 3,
%       "end_offset": 6,
%       "type": "word",
%       "position": 1
%     },
%     {
%       "token": "ได้",
%       "start_offset": 6,
%       "end_offset": 9,
%       "type": "word",
%       "position": 2
%     },
%     {
%       "token": "ต้อง",
%       "start_offset": 9,
%       "end_offset": 13,
%       "type": "word",
%       "position": 3
%     },
%     {
%       "token": "แสดง",
%       "start_offset": 13,
%       "end_offset": 17,
%       "type": "word",
%       "position": 4
%     },
%     {
%       "token": "ว่า",
%       "start_offset": 17,
%       "end_offset": 20,
%       "type": "word",
%       "position": 5
%     },
%     {
%       "token": "งาน",
%       "start_offset": 20,
%       "end_offset": 23,
%       "type": "word",
%       "position": 6
%     },
%     {
%       "token": "ดี",
%       "start_offset": 23,
%       "end_offset": 25,
%       "type": "word",
%       "position": 7
%     }
%   ]
% }
% ----------------------------
% 

The above sentence would produce the following terms:

```text
[ การ, ที่, ได้, ต้อง, แสดง, ว่า, งาน, ดี ]
```


## Configuration [_configuration_20]

The `thai` tokenizer is not configurable.

