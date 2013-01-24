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

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines how to perform suggesting. This builders allows a number of global options to be specified and
 * an arbitrary number of {@link org.elasticsearch.search.suggest.SuggestBuilder.FuzzySuggestion} instances.
 * <p/>
 * Suggesting works by suggesting terms that appear in the suggest text that are similar compared to the terms in
 * provided text. These spelling suggestions are based on several options described in this class.
 */
public class SuggestBuilder implements ToXContent {

    private String globalText;

    private final List<Suggestion> suggestions = new ArrayList<Suggestion>();

    /**
     * Sets the text to provide suggestions for. The suggest text is a required option that needs
     * to be set either via this setter or via the {@link org.elasticsearch.search.suggest.SuggestBuilder.Suggestion#setText(String)} method.
     * <p/>
     * The suggest text gets analyzed by the suggest analyzer or the suggest field search analyzer.
     * For each analyzed token, suggested terms are suggested if possible.
     */
    public SuggestBuilder setText(String globalText) {
        this.globalText = globalText;
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.search.suggest.SuggestBuilder.FuzzySuggestion} instance under a user defined name.
     * The order in which the <code>Suggestions</code> are added, is the same as in the response.
     */
    public SuggestBuilder addSuggestion(Suggestion suggestion) {
        suggestions.add(suggestion);
        return this;
    }

    /**
     * Returns all suggestions with the defined names.
     */
    public List<Suggestion> getSuggestion() {
        return suggestions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("suggest");
        if (globalText != null) {
            builder.field("text", globalText);
        }

        builder.startObject("suggestions");
        for (Suggestion suggestion : suggestions) {
            builder = suggestion.toXContent(builder, params);
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    /**
     * Convenience factory method.
     *
     * @param name The name of this suggestion. This is a required parameter.
     */
    public static FuzzySuggestion fuzzySuggestion(String name) {
        return new FuzzySuggestion(name);
    }

    public static abstract class Suggestion<T> implements ToXContent {

        private String name;
        private String suggester;
        private String text;

        public Suggestion(String name, String suggester) {
            this.name = name;
            this.suggester = suggester;
        }

        /**
         * Same as in {@link SuggestBuilder#setText(String)}, but in the suggestion scope.
         */
        public T setText(String text) {
            this.text = text;
            return (T) this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            if (suggester != null) {
                builder.field("suggester", suggester);
            }
            if (text != null) {
                builder.field("text", text);
            }
            builder = innerToXContent(builder, params);
            builder.endObject();
            return builder;
        }

        protected abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;
    }

    /**
     * Defines the actual suggest command. Each command uses the global options unless defined in the suggestion itself.
     * All options are the same as the global options, but are only applicable for this suggestion.
     */
    public static class FuzzySuggestion extends Suggestion<FuzzySuggestion> {

        private String field;
        private String analyzer;
        private String suggestMode;
        private Float accuracy;
        private Integer size;
        private String sort;
        private String stringDistance;
        private Boolean lowerCaseTerms;
        private Integer maxEdits;
        private Integer factor;
        private Float maxTermFreq;
        private Integer prefixLength;
        private Integer minWordLength;
        private Float minDocFreq;
        private Integer shardSize;

        /**
         * @param name The name of this suggestion. This is a required parameter.
         */
        public FuzzySuggestion(String name) {
            super(name, "fuzzy");
        }

        /**
         * Sets from what field to fetch the candidate suggestions from. This is an required option and needs to be set
         * via this setter or {@link org.elasticsearch.search.suggest.SuggestBuilder.FuzzySuggestion#setField(String)} method
         */
        public FuzzySuggestion setField(String field) {
            this.field = field;
            return this;
        }

        /**
         * Sets the analyzer to analyse to suggest text with. Defaults to the search analyzer of the suggest field.
         */
        public FuzzySuggestion setAnalyzer(String analyzer) {
            this.analyzer = analyzer;
            return this;
        }

        /**
         * The global suggest mode controls what suggested terms are included or controls for what suggest text tokens,
         * terms should be suggested for. Three possible values can be specified:
         * <ol>
         * <li><code>missing</code> - Only suggest terms in the suggest text that aren't in the index. This is the default.
         * <li><code>popular</code> - Only suggest terms that occur in more docs then the original suggest text term.
         * <li><code>always</code> - Suggest any matching suggest terms based on tokens in the suggest text.
         * </ol>
         */
        public FuzzySuggestion setSuggestMode(String suggestMode) {
            this.suggestMode = suggestMode;
            return this;
        }

        /**
         * Sets how similar the suggested terms at least need to be compared to the original suggest text tokens.
         * A value between 0 and 1 can be specified. This value will be compared to the string distance result of each
         * candidate spelling correction.
         * <p/>
         * Default is 0.5f.
         */
        public FuzzySuggestion setAccuracy(float accuracy) {
            this.accuracy = accuracy;
            return this;
        }

        /**
         * Sets the maximum suggestions to be returned per suggest text term.
         */
        public FuzzySuggestion setSize(int size) {
            if (size <= 0) {
                throw new ElasticSearchIllegalArgumentException("Size must be positive");
            }

            this.size = size;
            return this;
        }

        /**
         * Sets how to sort the suggest terms per suggest text token.
         * Two possible values:
         * <ol>
         * <li><code>score</code> - Sort should first be based on score, then document frequency and then the term itself.
         * <li><code>frequency</code> - Sort should first be based on document frequency, then scotr and then the term itself.
         * </ol>
         * <p/>
         * What the score is depends on the suggester being used.
         */
        public FuzzySuggestion setSort(String sort) {
            this.sort = sort;
            return this;
        }

        /**
         * Sets what string distance implementation to use for comparing how similar suggested terms are.
         * Four possible values can be specified:
         * <ol>
         * <li><code>internal</code> - This is the default and is based on <code>damerau_levenshtein</code>, but
         * highly optimized for comparing string distance for terms inside the index.
         * <li><code>damerau_levenshtein</code> - String distance algorithm based on Damerau-Levenshtein algorithm.
         * <li><code>levenstein</code> - String distance algorithm based on Levenstein edit distance algorithm.
         * <li><code>jarowinkler</code> - String distance algorithm based on Jaro-Winkler algorithm.
         * <li><code>ngram</code> - String distance algorithm based on n-grams.
         * </ol>
         */
        public FuzzySuggestion setStringDistance(String stringDistance) {
            this.stringDistance = stringDistance;
            return this;
        }

        /**
         * Sets whether to lowercase the suggest text tokens just before suggesting terms.
         */
        public FuzzySuggestion setLowerCaseTerms(Boolean lowerCaseTerms) {
            this.lowerCaseTerms = lowerCaseTerms;
            return this;
        }

        /**
         * Sets the maximum edit distance candidate suggestions can have in order to be considered as a suggestion.
         * Can only be a value between 1 and 2. Any other value result in an bad request error being thrown. Defaults to 2.
         */
        public FuzzySuggestion setMaxEdits(Integer maxEdits) {
            this.maxEdits = maxEdits;
            return this;
        }

        /**
         * A factor that is used to multiply with the size in order to inspect more candidate suggestions.
         * Can improve accuracy at the cost of performance. Defaults to 5.
         */
        public FuzzySuggestion setFactor(Integer factor) {
            this.factor = factor;
            return this;
        }

        /**
         * Sets a maximum threshold in number of documents a suggest text token can exist in order to be corrected.
         * Can be a relative percentage number (e.g 0.4) or an absolute number to represent document frequencies.
         * If an value higher than 1 is specified then fractional can not be specified. Defaults to 0.01f.
         * <p/>
         * This can be used to exclude high frequency terms from being suggested. High frequency terms are usually
         * spelled correctly on top of this this also improves the suggest performance.
         */
        public FuzzySuggestion setMaxTermFreq(float maxTermFreq) {
            this.maxTermFreq = maxTermFreq;
            return this;
        }

        /**
         * Sets the number of minimal prefix characters that must match in order be a candidate suggestion.
         * Defaults to 1. Increasing this number improves suggest performance. Usually misspellings don't occur in the
         * beginning of terms.
         */
        public FuzzySuggestion setPrefixLength(int prefixLength) {
            this.prefixLength = prefixLength;
            return this;
        }

        /**
         * The minimum length a suggest text term must have in order to be corrected. Defaults to 4.
         */
        public FuzzySuggestion setMinWordLength(int minWordLength) {
            this.minWordLength = minWordLength;
            return this;
        }

        /**
         * Sets a minimal threshold in number of documents a suggested term should appear in. This can be specified as
         * an absolute number or as a relative percentage of number of documents. This can improve quality by only suggesting
         * high frequency terms. Defaults to 0f and is not enabled. If a value higher than 1 is specified then the number
         * cannot be fractional.
         */
        public FuzzySuggestion setMinDocFreq(float minDocFreq) {
            this.minDocFreq = minDocFreq;
            return this;
        }

        /**
         * Sets the maximum number of suggested term to be retrieved from each individual shard. During the reduce
         * phase the only the top N suggestions are returned based on the <code>size</code> option. Defaults to the
         * <code>size</code> option.
         * <p/>
         * Setting this to a value higher than the `size` can be useful in order to get a more accurate document frequency
         * for suggested terms. Due to the fact that terms are partitioned amongst shards, the shard level document
         * frequencies of suggestions may not be precise. Increasing this will make these document frequencies
         * more precise.
         */
        public FuzzySuggestion setShardSize(Integer shardSize) {
            this.shardSize = shardSize;
            return this;
        }

        @Override
        public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (field != null) {
                builder.field("field", field);
            }
            if (suggestMode != null) {
                builder.field("suggest_mode", suggestMode);
            }
            if (accuracy != null) {
                builder.field("accuracy", accuracy);
            }
            if (size != null) {
                builder.field("size", size);
            }
            if (sort != null) {
                builder.field("sort", sort);
            }
            if (stringDistance != null) {
                builder.field("string_distance", stringDistance);
            }
            if (lowerCaseTerms != null) {
                builder.field("lowercase_terms", lowerCaseTerms);
            }
            if (maxEdits != null) {
                builder.field("max_edits", maxEdits);
            }
            if (factor != null) {
                builder.field("factor", factor);
            }
            if (maxTermFreq != null) {
                builder.field("max_term_freq", maxTermFreq);
            }
            if (prefixLength != null) {
                builder.field("prefix_length", prefixLength);
            }
            if (minWordLength != null) {
                builder.field("min_word_len", minWordLength);
            }
            if (minDocFreq != null) {
                builder.field("min_doc_freq", minDocFreq);
            }
            if (shardSize != null) {
                builder.field("shard_size", shardSize);
            }
            return builder;
        }
    }

}
