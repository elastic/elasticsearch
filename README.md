ICU Analysis for ElasticSearch
==================================

The ICU Analysis plugin integrates Lucene ICU module into elasticsearch, adding ICU relates analysis components.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-analysis-icu/1.11.0`.


<table>
	<thead>
		<tr>
			<td>ICU Analysis Plugin</td>
			<td>Elasticsearch</td>
			<td>Release date</td>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td>1.12.0-SNAPSHOT (master)</td>
			<td>0.90.6 -> master</td>
			<td></td>
		</tr>
		<tr>
			<td>1.11.0</td>
			<td>0.90.3 -> 0.90.5</td>
			<td>2013-08-08</td>
		</tr>
		<tr>
			<td>1.10.0</td>
			<td>0.90.1 -> 0.90.2</td>
			<td>2013-05-30</td>
		</tr>
		<tr>
			<td>1.9.0</td>
			<td>0.90.0</td>
			<td>2013-04-29</td>
		</tr>
		<tr>
			<td>1.8.0</td>
			<td>0.90.0</td>
			<td>2013-02-26</td>
		</tr>
		<tr>
			<td>1.7.0</td>
			<td>0.19 -> 0.20</td>
			<td>2012-09-28</td>
		</tr>
		<tr>
			<td>1.6.0</td>
			<td>0.19 -> 0.20</td>
			<td>2012-09-27</td>
		</tr>
		<tr>
			<td>1.5.0</td>
			<td>0.19 -> 0.20</td>
			<td>2012-04-30</td>
		</tr>
		<tr>
			<td>1.4.0</td>
			<td>0.19 -> 0.20</td>
			<td>2012-03-20</td>
		</tr>
		<tr>
			<td>1.3.0</td>
			<td>0.19 -> 0.20</td>
			<td>2012-03-20</td>
		</tr>
		<tr>
			<td>1.2.0</td>
			<td>0.19 -> 0.20</td>
			<td>2012-02-07</td>
		</tr>
		<tr>
			<td>1.1.0</td>
			<td>0.18</td>
			<td>2011-12-13</td>
		</tr>
		<tr>
			<td>1.0.0</td>
			<td>0.18</td>
			<td>2011-12-05</td>
		</tr>
	</tbody>
</table>


ICU Normalization
-----------------

Normalizes characters as explained "here":http://userguide.icu-project.org/transforms/normalization. It registers itself by default under `icu_normalizer` or `icuNormalizer` using the default settings. Allows for the name parameter to be provided which can include the following values: `nfc`, `nfkc`, and `nfkc_cf`. Here is a sample settings:

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "collation" : {
                        "tokenizer" : "keyword",
                        "filter" : ["icu_normalizer"]
                    }
                }
            }
        }
    }

ICU Folding
-----------

Folding of unicode characters based on `UTR#30`. It registers itself under `icu_folding` and `icuFolding` names. Sample setting:

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "collation" : {
                        "tokenizer" : "keyword",
                        "filter" : ["icu_folding"]
                    }
                }
            }
        }
    }

ICU Filtering
-------------

The folding can be filtered by a set of unicode characters with the parameter `unicodeSetFilter`. This is useful for a non-internationalized search engine where retaining a set of national characters which are primary letters in a specific language is wanted. See syntax for the UnicodeSet "here":http://icu-project.org/apiref/icu4j/com/ibm/icu/text/UnicodeSet.html.

The Following example exempts Swedish characters from the folding. Note that the filtered characters are NOT lowercased which is why we add that filter below.

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "folding" : {
                        "tokenizer" : "standard",
                        "filter" : ["my_icu_folding", "lowercase"]
                    }
                }
                "filter" : {
                    "my_icu_folding" : {
                        "type" : "icu_folding"
                        "unicodeSetFilter" : "[^åäöÅÄÖ]"
                    }
                }
            }
        }
    }

ICU Collation
-------------

Uses collation token filter. Allows to either specify the rules for collation (defined "here":http://www.icu-project.org/userguide/Collate_Customization.html) using the `rules` parameter (can point to a location or expressed in the settings, location can be relative to config location), or using the `language` parameter (further specialized by country and variant). By default registers under `icu_collation` or `icuCollation` and uses the default locale.

Here is a sample settings:

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "collation" : {
                        "tokenizer" : "keyword",
                        "filter" : ["icu_collation"]
                    }
                }
            }
        }
    }

And here is a sample of custom collation:

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "collation" : {
                        "tokenizer" : "keyword",
                        "filter" : ["myCollator"]
                    }
                },
                "filter" : {
                    "myCollator" : {
                        "type" : "icu_collation",
                        "language" : "en"
                    }
                }
            }
        }
    }

Optional options:
* `strength` - The strength property determines the minimum level of difference considered significant during comparison.
 The default strength for the Collator is `tertiary`, unless specified otherwise by the locale used to create the Collator.
 Possible values: `primary`, `secondary`, `tertiary`, `quaternary` or `identical`.
 See ICU Collation:http://icu-project.org/apiref/icu4j/com/ibm/icu/text/Collator.html documentation for a more detailed
 explanation for the specific values.
* `decomposition` - Possible values: `no` or `canonical`. Defaults to `no`. Setting this decomposition property with
`canonical` allows the Collator to handle un-normalized text properly, producing the same results as if the text were
normalized. If `no` is set, it is the user's responsibility to insure that all text is already in the appropriate form
before a comparison or before getting a CollationKey. Adjusting decomposition mode allows the user to select between
faster and more complete collation behavior. Since a great many of the world's languages do not require text
normalization, most locales set `no` as the default decomposition mode.

Expert options:
* `alternate` - Possible values: `shifted` or `non-ignorable`. Sets the alternate handling for strength `quaternary`
 to be either shifted or non-ignorable. What boils down to ignoring punctuation and whitespace.
* `caseLevel` - Possible values: `true` or `false`. Default is `false`. Whether case level sorting is required. When
 strength is set to `primary` this will ignore accent differences.
* `caseFirst` - Possible values: `lower` or `upper`. Useful to control which case is sorted first when case is not ignored
 for strength `tertiary`.
* `numeric` - Possible values: `true` or `false`. Whether digits are sorted according to numeric representation. For
 example the value `egg-9` is sorted before the value `egg-21`. Defaults to `false`.
* `variableTop` - Single character or contraction. Controls what is variable for `alternate`.
* `hiraganaQuaternaryMode` - Possible values: `true` or `false`. Defaults to `false`. Distinguishing between Katakana
 and Hiragana characters in `quaternary` strength .

ICU Tokenizer
-------------

Breaks text into words according to UAX #29: Unicode Text Segmentation ((http://www.unicode.org/reports/tr29/)).

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "collation" : {
                        "tokenizer" : "icu_tokenizer",
                    }
                }
            }
        }
    }


License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2013 Shay Banon and ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
