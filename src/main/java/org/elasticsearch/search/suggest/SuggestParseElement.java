/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.suggest;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.spell.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class SuggestParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        SuggestionSearchContext suggestionSearchContext = new SuggestionSearchContext();

        BytesRef globalText = null;

        Analyzer defaultAnalyzer = context.mapperService().searchAnalyzer();
        float defaultAccuracy = SpellChecker.DEFAULT_ACCURACY;
        int defaultSize = 5;
        SuggestMode defaultSuggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
        Suggest.Suggestion.Sort defaultSort = Suggest.Suggestion.Sort.SCORE;
        StringDistance defaultStringDistance = DirectSpellChecker.INTERNAL_LEVENSHTEIN;
        boolean defaultLowerCaseTerms = false; // changed from Lucene default because we rely on search analyzer to properly handle it
        int defaultMaxEdits = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;
        int defaultFactor = 5;
        float defaultMaxTermFreq = 0.01f;
        int defaultPrefixLength = 1;
        int defaultMinQueryLength = 4;
        float defaultMinDocFreq = 0f;

        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("text".equals(fieldName)) {
                    globalText = parser.bytes();
                } else {
                    throw new ElasticSearchIllegalArgumentException("[suggest] does not support [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                // TODO: Once we have more suggester impls we need to have different parsing logic per suggester.
                // This code is now specific for the fuzzy suggester
                if ("suggestions".equals(fieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            SuggestionSearchContext.Suggestion suggestion = new SuggestionSearchContext.Suggestion();
                            suggestionSearchContext.addSuggestion(fieldName, suggestion);

                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    fieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if ("suggester".equals(fieldName)) {
                                        suggestion.suggester(parser.text());
                                    } else if ("analyzer".equals(fieldName)) {
                                        String analyzerName = parser.text();
                                        Analyzer analyzer = context.mapperService().analysisService().analyzer(analyzerName);
                                        if (analyzer == null) {
                                            throw new ElasticSearchIllegalArgumentException("Analyzer [" + analyzerName + "] doesn't exists");
                                        }
                                        suggestion.analyzer(analyzer);
                                    } else if ("text".equals(fieldName)) {
                                        suggestion.text(parser.bytes());
                                    } else if ("field".equals(fieldName)) {
                                        suggestion.setField(parser.text());
                                    } else if ("accuracy".equals(fieldName)) {
                                        suggestion.accuracy(parser.floatValue());
                                    } else if ("size".equals(fieldName)) {
                                        suggestion.size(parser.intValue());
                                    } else if ("suggest_mode".equals(fieldName) || "suggestMode".equals(fieldName)) {
                                        suggestion.suggestMode(resolveSuggestMode(parser.text()));
                                    } else if ("sort".equals(fieldName)) {
                                        suggestion.sort(resolveSort(parser.text()));
                                    } else if ("string_distance".equals(fieldName) || "stringDistance".equals(fieldName)) {
                                        suggestion.stringDistance(resolveDistance(parser.text()));
                                    } else if ("lowercase_terms".equals(fieldName) || "lowercaseTerms".equals(fieldName)) {
                                        suggestion.lowerCaseTerms(parser.booleanValue());
                                    } else if ("max_edits".equals(fieldName) || "maxEdits".equals(fieldName) || "fuzziness".equals(fieldName)) {
                                        suggestion.maxEdits(parser.intValue());
                                        if (suggestion.maxEdits() < 1 || suggestion.maxEdits() > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
                                            throw new ElasticSearchIllegalArgumentException("Illegal max_edits value " + suggestion.maxEdits());
                                        }
                                    } else if ("factor".equals(fieldName)) {
                                        suggestion.factor(parser.intValue());
                                    } else if ("max_term_freq".equals(fieldName) || "maxTermFreq".equals(fieldName)) {
                                        suggestion.maxTermFreq(parser.floatValue());
                                    } else if ("prefix_length".equals(fieldName) || "prefixLength".equals(fieldName)) {
                                        suggestion.prefixLength(parser.intValue());
                                    } else if ("min_word_len".equals(fieldName) || "minWordLen".equals(fieldName)) {
                                        suggestion.minQueryLength(parser.intValue());
                                    } else if ("min_doc_freq".equals(fieldName) || "minDocFreq".equals(fieldName)) {
                                        suggestion.minDocFreq(parser.floatValue());
                                    } else if ("shard_size".equals(fieldName) || "shardSize".equals(fieldName)) {
                                        suggestion.shardSize(parser.intValue());
                                    } else {
                                        throw new ElasticSearchIllegalArgumentException("suggester[fuzzy] doesn't support [" + fieldName + "]");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Verify options and set defaults
        for (SuggestionSearchContext.Suggestion command : suggestionSearchContext.suggestions().values()) {
            if (command.suggester() == null) {
                throw new ElasticSearchIllegalArgumentException("The required suggester option is missing");
            }
            if (command.field() == null) {
                throw new ElasticSearchIllegalArgumentException("The required field option is missing");
            }

            if (command.text() == null) {
                if (globalText == null) {
                    throw new ElasticSearchIllegalArgumentException("The required text option is missing");
                }

                command.text(globalText);
            }
            if (command.analyzer() == null) {
                command.analyzer(defaultAnalyzer);
            }
            if (command.accuracy() == null) {
                command.accuracy(defaultAccuracy);
            }
            if (command.size() == null) {
                command.size(defaultSize);
            }
            if (command.suggestMode() == null) {
                command.suggestMode(defaultSuggestMode);
            }
            if (command.sort() == null) {
                command.sort(defaultSort);
            }
            if (command.stringDistance() == null) {
                command.stringDistance(defaultStringDistance);
            }
            if (command.lowerCaseTerms() == null) {
                command.lowerCaseTerms(defaultLowerCaseTerms);
            }
            if (command.maxEdits() == null) {
                command.maxEdits(defaultMaxEdits);
            }
            if (command.factor() == null) {
                command.factor(defaultFactor);
            }
            if (command.maxTermFreq() == null) {
                command.maxTermFreq(defaultMaxTermFreq);
            }
            if (command.prefixLength() == null) {
                command.prefixLength(defaultPrefixLength);
            }
            if (command.minWordLength() == null) {
                command.minQueryLength(defaultMinQueryLength);
            }
            if (command.minDocFreq() == null) {
                command.minDocFreq(defaultMinDocFreq);
            }
            if (command.shardSize() == null) {
                command.shardSize(defaultSize);
            }
        }
        context.suggest(suggestionSearchContext);
    }

    private SuggestMode resolveSuggestMode(String sortVal) {
        if ("missing".equals(sortVal)) {
            return SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
        } else if ("popular".equals(sortVal)) {
            return SuggestMode.SUGGEST_MORE_POPULAR;
        } else if ("always".equals(sortVal)) {
            return SuggestMode.SUGGEST_ALWAYS;
        } else {
            throw new ElasticSearchIllegalArgumentException("Illegal suggest mode " + sortVal);
        }
    }

    private Suggest.Suggestion.Sort resolveSort(String sortVal) {
        if ("score".equals(sortVal)) {
            return Suggest.Suggestion.Sort.SCORE;
        } else if ("frequency".equals(sortVal)) {
            return Suggest.Suggestion.Sort.FREQUENCY;
        } else {
            throw new ElasticSearchIllegalArgumentException("Illegal suggest sort " + sortVal);
        }
    }

    private StringDistance resolveDistance(String distanceVal) {
        if ("internal".equals(distanceVal)) {
            return DirectSpellChecker.INTERNAL_LEVENSHTEIN;
        } else if ("damerau_levenshtein".equals(distanceVal)) {
            return new LuceneLevenshteinDistance();
        } else if ("levenstein".equals(distanceVal)) {
            return new LevensteinDistance();
        } else if ("jarowinkler".equals(distanceVal)) {
            return new JaroWinklerDistance();
        } else if ("ngram".equals(distanceVal)) {
            return new NGramDistance();
        } else {
            throw new ElasticSearchIllegalArgumentException("Illegal distance option " + distanceVal);
        }
    }

}
