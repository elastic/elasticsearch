---
navigation_title: "MinHash"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-minhash-tokenfilter.html
---

# MinHash token filter [analysis-minhash-tokenfilter]


Uses the [MinHash](https://en.wikipedia.org/wiki/MinHash) technique to produce a signature for a token stream. You can use MinHash signatures to estimate the similarity of documents. See [Using the `min_hash` token filter for similarity search](#analysis-minhash-tokenfilter-similarity-search).

The `min_hash` filter performs the following operations on a token stream in order:

1. Hashes each token in the stream.
2. Assigns the hashes to buckets, keeping only the smallest hashes of each bucket.
3. Outputs the smallest hash from each bucket as a token stream.

This filter uses Lucene’s [MinHashFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/minhash/MinHashFilter.md).

## Configurable parameters [analysis-minhash-tokenfilter-configure-parms]

`bucket_count`
:   (Optional, integer) Number of buckets to which hashes are assigned. Defaults to `512`.

`hash_count`
:   (Optional, integer) Number of ways to hash each token in the stream. Defaults to `1`.

`hash_set_size`
:   (Optional, integer) Number of hashes to keep from each bucket. Defaults to `1`.

    Hashes are retained by ascending size, starting with the bucket’s smallest hash first.


`with_rotation`
:   (Optional, Boolean) If `true`, the filter fills empty buckets with the value of the first non-empty bucket to its circular right if the `hash_set_size` is `1`. If the `bucket_count` argument is greater than `1`, this parameter defaults to `true`. Otherwise, this parameter defaults to `false`.


## Tips for configuring the `min_hash` filter [analysis-minhash-tokenfilter-configuration-tips]

* `min_hash` filter input tokens should typically be k-words shingles produced from [shingle token filter](/reference/text-analysis/analysis-shingle-tokenfilter.md). You should choose `k` large enough so that the probability of any given shingle occurring in a document is low. At the same time, as internally each shingle is hashed into to 128-bit hash, you should choose `k` small enough so that all possible different k-words shingles can be hashed to 128-bit hash with minimal collision.
* We recommend you test different arguments for the `hash_count`, `bucket_count` and `hash_set_size` parameters:

    * To improve precision, increase the `bucket_count` or `hash_set_size` arguments. Higher `bucket_count` and `hash_set_size` values increase the likelihood that different tokens are indexed to different buckets.
    * To improve the recall, increase the value of the `hash_count` argument. For example, setting `hash_count` to `2` hashes each token in two different ways, increasing the number of potential candidates for search.

* By default, the `min_hash` filter produces 512 tokens for each document. Each token is 16 bytes in size. This means each document’s size will be increased by around 8Kb.
* The `min_hash` filter is used for Jaccard similarity. This means that it doesn’t matter how many times a document contains a certain token, only that if it contains it or not.


## Using the `min_hash` token filter for similarity search [analysis-minhash-tokenfilter-similarity-search]

The `min_hash` token filter allows you to hash documents for similarity search. Similarity search, or nearest neighbor search is a complex problem. A naive solution requires an exhaustive pairwise comparison between a query document and every document in an index. This is a prohibitive operation if the index is large. A number of approximate nearest neighbor search solutions have been developed to make similarity search more practical and computationally feasible. One of these solutions involves hashing of documents.

Documents are hashed in a way that similar documents are more likely to produce the same hash code and are put into the same hash bucket, while dissimilar documents are more likely to be hashed into different hash buckets. This type of hashing is known as locality sensitive hashing (LSH).

Depending on what constitutes the similarity between documents, various LSH functions [have been proposed](https://arxiv.org/abs/1408.2927). For [Jaccard similarity](https://en.wikipedia.org/wiki/Jaccard_index), a popular LSH function is [MinHash](https://en.wikipedia.org/wiki/MinHash). A general idea of the way MinHash produces a signature for a document is by applying a random permutation over the whole index vocabulary (random numbering for the vocabulary), and recording the minimum value for this permutation for the document (the minimum number for a vocabulary word that is present in the document). The permutations are run several times; combining the minimum values for all of them will constitute a signature for the document.

In practice, instead of random permutations, a number of hash functions are chosen. A hash function calculates a hash code for each of a document’s tokens and chooses the minimum hash code among them. The minimum hash codes from all hash functions are combined to form a signature for the document.


## Customize and add to an analyzer [analysis-minhash-tokenfilter-customize]

To customize the `min_hash` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the following custom token filters to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md):

* `my_shingle_filter`, a custom [`shingle` filter](/reference/text-analysis/analysis-shingle-tokenfilter.md). `my_shingle_filter` only outputs five-word shingles.
* `my_minhash_filter`, a custom `min_hash` filter. `my_minhash_filter` hashes each five-word shingle once. It then assigns the hashes into 512 buckets, keeping only the smallest hash from each bucket.

The request also assigns the custom analyzer to the `fingerprint` field mapping.

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "filter": {
        "my_shingle_filter": {      <1>
          "type": "shingle",
          "min_shingle_size": 5,
          "max_shingle_size": 5,
          "output_unigrams": false
        },
        "my_minhash_filter": {
          "type": "min_hash",
          "hash_count": 1,          <2>
          "bucket_count": 512,      <3>
          "hash_set_size": 1,       <4>
          "with_rotation": true     <5>
        }
      },
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "standard",
          "filter": [
            "my_shingle_filter",
            "my_minhash_filter"
          ]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "fingerprint": {
        "type": "text",
        "analyzer": "my_analyzer"
      }
    }
  }
}
```

1. Configures a custom shingle filter to output only five-word shingles.
2. Each five-word shingle in the stream is hashed once.
3. The hashes are assigned to 512 buckets.
4. Only the smallest hash in each bucket is retained.
5. The filter fills empty buckets with the values of neighboring buckets.



