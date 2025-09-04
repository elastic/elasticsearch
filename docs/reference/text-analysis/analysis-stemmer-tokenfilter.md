---
navigation_title: "Stemmer"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-stemmer-tokenfilter.html
---

# Stemmer token filter [analysis-stemmer-tokenfilter]


Provides [algorithmic stemming](docs-content://manage-data/data-store/text-analysis/stemming.md#algorithmic-stemmers) for several languages, some with additional variants. For a list of supported languages, see the [`language`](#analysis-stemmer-tokenfilter-language-parm) parameter.

When not customized, the filter uses the [porter stemming algorithm](https://snowballstem.org/algorithms/porter/stemmer.html) for English.

## Example [analysis-stemmer-tokenfilter-analyze-ex]

The following analyze API request uses the `stemmer` filter’s default porter stemming algorithm to stem `the foxes jumping quickly` to `the fox jump quickli`:

```console
GET /_analyze
{
  "tokenizer": "standard",
  "filter": [ "stemmer" ],
  "text": "the foxes jumping quickly"
}
```

The filter produces the following tokens:

```text
[ the, fox, jump, quickli ]
```


## Add to an analyzer [analysis-stemmer-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `stemmer` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "whitespace",
          "filter": [ "stemmer" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-stemmer-tokenfilter-configure-parms]

$$$analysis-stemmer-tokenfilter-language-parm$$$

`language`
:   (Optional, string) Language-dependent stemming algorithm used to stem tokens. If both this and the `name` parameter are specified, the `language` parameter argument is used.

:::{dropdown} Valid values for language

Valid values are sorted by language. Defaults to [**`english`**](https://snowballstem.org/algorithms/porter/stemmer.html). Recommended algorithms are **bolded**.

* Arabic: [**`arabic`**](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/ar/ArabicStemmer.md)
* Armenian: [**`armenian`**](https://snowballstem.org/algorithms/armenian/stemmer.md)
* Basque: [**`basque`**](https://snowballstem.org/algorithms/basque/stemmer.md)
* Bengali:[**`bengali`**](https://www.tandfonline.com/doi/abs/10.1080/02564602.1993.11437284)
* Brazilian Portuguese:[**`brazilian`**](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/br/BrazilianStemmer.md)
* Bulgarian:[**`bulgarian`**](http://members.unine.ch/jacques.savoy/Papers/BUIR.pdf)
* Catalan:[**`catalan`**](https://snowballstem.org/algorithms/catalan/stemmer.md)
* Czech:[**`czech`**](https://dl.acm.org/doi/10.1016/j.ipm.2009.06.001)
* Danish:[**`danish`**](https://snowballstem.org/algorithms/danish/stemmer.md)
* Dutch
  * [**`dutch`**](https://snowballstem.org/algorithms/dutch/stemmer.md)
  * [`dutch_kp`](https://snowballstem.org/algorithms/kraaij_pohlmann/stemmer.md)
    :::{admonition} Deprecated in 8.16.0
    This language was deprecated in 8.16.0.
    :::
* English:
  * [**`english`**](https://snowballstem.org/algorithms/porter/stemmer.html)
  * [`light_english`](https://ciir.cs.umass.edu/pubfiles/ir-35.pdf)
  * [`lovins`](https://snowballstem.org/algorithms/lovins/stemmer.md)
    :::{admonition} Deprecated in 8.16.0
    This language was deprecated in 8.16.0.
    :::
  * [`minimal_english`](https://www.researchgate.net/publication/220433848_How_effective_is_suffixing)
  * [`porter2`](https://snowballstem.org/algorithms/english/stemmer.html)
  * [`possessive_english`](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/en/EnglishPossessiveFilter.html)
* Estonian:[**`estonian`**](https://lucene.apache.org/core/10_0_0/analyzers-common/org/tartarus/snowball/ext/EstonianStemmer.md)
* Finnish:
  * [**`finnish`**](https://snowballstem.org/algorithms/finnish/stemmer.html)
  * [`light_finnish`](http://clef.isti.cnr.it/2003/WN_web/22.pdf)
* French:
  * [**`light_french`**](https://dl.acm.org/citation.cfm?id=1141523)
  * [`french`](https://snowballstem.org/algorithms/french/stemmer.html)
  * [`minimal_french`](https://dl.acm.org/citation.cfm?id=318984)
* Galician:
  * [**`galician`**](http://bvg.udc.es/recursos_lingua/stemming.jsp)
  * [`minimal_galician`](http://bvg.udc.es/recursos_lingua/stemming.jsp) (Plural step only)
* German:
  * [**`light_german`**](https://dl.acm.org/citation.cfm?id=1141523),
  * [`german`](https://snowballstem.org/algorithms/german/stemmer.html)
  * [`minimal_german`](http://members.unine.ch/jacques.savoy/clef/morpho.pdf)
* Greek:[**`greek`**](https://sais.se/mthprize/2007/ntais2007.pdf)
* Hindi:[**`hindi`**](http://computing.open.ac.uk/Sites/EACLSouthAsia/Papers/p6-Ramanathan.pdf)
* Hungarian:
  * [**`hungarian`**](https://snowballstem.org/algorithms/hungarian/stemmer.html)
  * [`light_hungarian`](https://dl.acm.org/citation.cfm?id=1141523&dl=ACM&coll=DL&CFID=179095584&CFTOKEN=80067181)
* Indonesian:[**`indonesian`**](http://www.illc.uva.nl/Publications/ResearchReports/MoL-2003-02.text.pdf)
* Irish:[**`irish`**](https://snowballstem.org/otherapps/oregan/)
* Italian:
  * [**`light_italian`**](https://www.ercim.eu/publication/ws-proceedings/CLEF2/savoy.pdf)
  * [`italian`](https://snowballstem.org/algorithms/italian/stemmer.html)
* Kurdish (Sorani):[**`sorani`**](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/ckb/SoraniStemmer.md)
* Latvian:[**`latvian`**](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/lv/LatvianStemmer.md)
* Lithuanian:[**`lithuanian`**](https://svn.apache.org/viewvc/lucene/dev/branches/lucene_solr_5_3/lucene/analysis/common/src/java/org/apache/lucene/analysis/lt/stem_ISO_8859_1.sbl?view=markup)
* Norwegian (Bokmål):
  * [**`norwegian`**](https://snowballstem.org/algorithms/norwegian/stemmer.html)
  * [**`light_norwegian`**](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/no/NorwegianLightStemmer.md)
  * [`minimal_norwegian`](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/no/NorwegianMinimalStemmer.md)
* Norwegian (Nynorsk):
  * [**`light_nynorsk`**](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/no/NorwegianLightStemmer.md)
    * [`minimal_nynorsk`](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/no/NorwegianMinimalStemmer.md)
* Persian:[**`persian`**](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/fa/PersianStemmer.md)
* Portuguese:
  * [**`light_portuguese`**](https://dl.acm.org/citation.cfm?id=1141523&dl=ACM&coll=DL&CFID=179095584&CFTOKEN=80067181)
  * [`minimal_portuguese`](http://www.inf.ufrgs.br/~buriol/papers/Orengo_CLEF07.pdf)
  * [`portuguese`](https://snowballstem.org/algorithms/portuguese/stemmer.html)
  * [`portuguese_rslp`](https://www.inf.ufrgs.br/\~viviane/rslp/index.htm)
* Romanian:[**`romanian`**](https://snowballstem.org/algorithms/romanian/stemmer.html)
* Russian:
  * [**`russian`**](https://snowballstem.org/algorithms/russian/stemmer.html)
  * [`light_russian`](https://doc.rero.ch/lm.php?url=1000%2C43%2C4%2C20091209094227-CA%2FDolamic_Ljiljana_-_Indexing_and_Searching_Strategies_for_the_Russian_20091209.pdf)
* Serbian:[**`serbian`**](https://snowballstem.org/algorithms/serbian/stemmer.html)
* Spanish:
  * [**`light_spanish`**](https://www.ercim.eu/publication/ws-proceedings/CLEF2/savoy.pdf)
  * [`spanish`](https://snowballstem.org/algorithms/spanish/stemmer.html)
  * [`spanish_plural`](https://www.wikilengua.org/index.php/Plural_(formaci%C3%B3n))
* Swedish:
  * [**`swedish`**](https://snowballstem.org/algorithms/swedish/stemmer.html)
  * [`light_swedish`](http://clef.isti.cnr.it/2003/WN_web/22.pdf)
* Turkish:[**`turkish`**](https://snowballstem.org/algorithms/turkish/stemmer.html)
:::

`name`
:   An alias for the [`language`](#analysis-stemmer-tokenfilter-language-parm) parameter. If both this and the `language` parameter are specified, the `language` parameter argument is used.


## Customize [analysis-stemmer-tokenfilter-customize]

To customize the `stemmer` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom `stemmer` filter that stems words using the `light_german` algorithm:

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "my_stemmer"
          ]
        }
      },
      "filter": {
        "my_stemmer": {
          "type": "stemmer",
          "language": "light_german"
        }
      }
    }
  }
}
```


