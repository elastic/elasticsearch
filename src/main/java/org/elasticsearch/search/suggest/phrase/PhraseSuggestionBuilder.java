/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Defines the actual suggest command for phrase suggestions ( <tt>phrase</tt>).
 */
public final class PhraseSuggestionBuilder extends SuggestionBuilder<PhraseSuggestionBuilder> {
    private Float maxErrors;
    private String separator;
    private Float realWordErrorLikelihood;
    private Float confidence;
    private final Map<String, List<CandidateGenerator>> generators = new HashMap<>();
    private Integer gramSize;
    private SmoothingModel model;
    private Boolean forceUnigrams;
    private Integer tokenLimit;
    private String preTag;
    private String postTag;
    private String collateQuery;
    private String collateFilter;
    private String collatePreference;
    private Map<String, Object> collateParams;
    private Boolean collatePrune;

    public PhraseSuggestionBuilder(String name) {
        super(name, "phrase");
    }

    /**
     * Sets the gram size for the n-gram model used for this suggester. The
     * default value is <tt>1</tt> corresponding to <tt>unigrams</tt>. Use
     * <tt>2</tt> for <tt>bigrams</tt> and <tt>3</tt> for <tt>trigrams</tt>.
     */
    public PhraseSuggestionBuilder gramSize(int gramSize) {
        if (gramSize < 1) {
            throw new ElasticsearchIllegalArgumentException("gramSize must be >= 1");
        }
        this.gramSize = gramSize;
        return this;
    }

    /**
     * Sets the maximum percentage of the terms that at most considered to be
     * misspellings in order to form a correction. This method accepts a float
     * value in the range [0..1) as a fraction of the actual query terms a
     * number <tt>&gt;=1</tt> as an absolut number of query terms.
     * 
     * The default is set to <tt>1.0</tt> which corresponds to that only
     * corrections with at most 1 missspelled term are returned.
     */
    public PhraseSuggestionBuilder maxErrors(Float maxErrors) {
        this.maxErrors = maxErrors;
        return this;
    }

    /**
     * Sets the separator that is used to separate terms in the bigram field. If
     * not set the whitespace character is used as a separator.
     */
    public PhraseSuggestionBuilder separator(String separator) {
        this.separator = separator;
        return this;
    }

    /**
     * Sets the likelihood of a term being a misspelled even if the term exists
     * in the dictionary. The default it <tt>0.95</tt> corresponding to 5% or
     * the real words are misspelled.
     */
    public PhraseSuggestionBuilder realWordErrorLikelihood(Float realWordErrorLikelihood) {
        this.realWordErrorLikelihood = realWordErrorLikelihood;
        return this;
    }

    /**
     * Sets the confidence level for this suggester. The confidence level
     * defines a factor applied to the input phrases score which is used as a
     * threshold for other suggest candidates. Only candidates that score higher
     * than the threshold will be included in the result. For instance a
     * confidence level of <tt>1.0</tt> will only return suggestions that score
     * higher than the input phrase. If set to <tt>0.0</tt> the top N candidates
     * are returned. The default is <tt>1.0</tt>
     */
    public PhraseSuggestionBuilder confidence(Float confidence) {
        this.confidence = confidence;
        return this;
    }

    /**
     * Adds a {@link CandidateGenerator} to this suggester. The
     * {@link CandidateGenerator} is used to draw candidates for each individual
     * phrase term before the candidates are scored.
     */
    public PhraseSuggestionBuilder addCandidateGenerator(CandidateGenerator generator) {
        List<CandidateGenerator> list = this.generators.get(generator.getType());
        if (list == null) {
            list = new ArrayList<>();
            this.generators.put(generator.getType(), list);
        }
        list.add(generator);
        return this;
    }

    /**
     * Clear the candidate generators.
     */
    public PhraseSuggestionBuilder clearCandidateGenerators() {
        this.generators.clear();
        return this;
    }
    
    /**
     * If set to <code>true</code> the phrase suggester will fail if the analyzer only
     * produces ngrams. the default it <code>true</code>.
     */
    public PhraseSuggestionBuilder forceUnigrams(boolean forceUnigrams) {
        this.forceUnigrams = forceUnigrams; 
        return this;
    }

    /**
     * Sets an explicit smoothing model used for this suggester. The default is
     * {@link PhraseSuggester#StupidBackoff}.
     */
    public PhraseSuggestionBuilder smoothingModel(SmoothingModel model) {
        this.model = model;
        return this;
    }
    
    public PhraseSuggestionBuilder tokenLimit(int tokenLimit) {
        this.tokenLimit = tokenLimit;
        return this;
    }

    /**
     * Setup highlighting for suggestions.  If this is called a highlight field
     * is returned with suggestions wrapping changed tokens with preTag and postTag.
     */
    public PhraseSuggestionBuilder highlight(String preTag, String postTag) {
        if ((preTag == null) != (postTag == null)) {
            throw new ElasticsearchIllegalArgumentException("Pre and post tag must both be null or both not be null.");
        }
        this.preTag = preTag;
        this.postTag = postTag;
        return this;
    }

    /**
     * Sets a query used for filtering out suggested phrases (collation).
     */
    public PhraseSuggestionBuilder collateQuery(String collateQuery) {
        this.collateQuery = collateQuery;
        return this;
    }

    /**
     * Sets a filter used for filtering out suggested phrases (collation).
     */
    public PhraseSuggestionBuilder collateFilter(String collateFilter) {
        this.collateFilter = collateFilter;
        return this;
    }

    /**
     * Sets routing preferences for executing filter query (collation).
     */
    public PhraseSuggestionBuilder collatePreference(String collatePreference) {
        this.collatePreference = collatePreference;
        return this;
    }

    /**
     * Sets additional params for collate script
     */
    public PhraseSuggestionBuilder collateParams(Map<String, Object> collateParams) {
        this.collateParams = collateParams;
        return this;
    }

    /**
     * Sets whether to prune suggestions after collation
     */
    public PhraseSuggestionBuilder collatePrune(boolean collatePrune) {
        this.collatePrune = collatePrune;
        return this;
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (realWordErrorLikelihood != null) {
            builder.field("real_word_error_likelihood", realWordErrorLikelihood);
        }
        if (confidence != null) {
            builder.field("confidence", confidence);
        }
        if (separator != null) {
            builder.field("separator", separator);
        }
        if (maxErrors != null) {
            builder.field("max_errors", maxErrors);
        }
        if (gramSize != null) {
            builder.field("gram_size", gramSize);
        }
        if (forceUnigrams != null) {
            builder.field("force_unigrams", forceUnigrams);
        }
        if (tokenLimit != null) {
            builder.field("token_limit", tokenLimit);
        }
        if (!generators.isEmpty()) {
            Set<Entry<String, List<CandidateGenerator>>> entrySet = generators.entrySet();
            for (Entry<String, List<CandidateGenerator>> entry : entrySet) {
                builder.startArray(entry.getKey());
                for (CandidateGenerator generator : entry.getValue()) {
                    generator.toXContent(builder, params);
                }
                builder.endArray();
            }
        }
        if (model != null) {
            builder.startObject("smoothing");
            model.toXContent(builder, params);
            builder.endObject();
        }
        if (preTag != null) {
            builder.startObject("highlight");
            builder.field("pre_tag", preTag);
            builder.field("post_tag", postTag);
            builder.endObject();
        }
        if (collateQuery != null || collateFilter != null) {
            builder.startObject("collate");
            if (collateQuery != null) {
                builder.field("query", collateQuery);
            }
            if (collateFilter != null) {
                builder.field("filter", collateFilter);
            }
            if (collatePreference != null) {
                builder.field("preference", collatePreference);
            }
            if (collateParams != null) {
                builder.field("params", collateParams);
            }
            if (collatePrune != null) {
                builder.field("prune", collatePrune.booleanValue());
            }
            builder.endObject();
        }
        return builder;
    }

    /**
     * Creates a new {@link DirectCandidateGenerator}
     * 
     * @param field
     *            the field this candidate generator operates on.
     */
    public static DirectCandidateGenerator candidateGenerator(String field) {
        return new DirectCandidateGenerator(field);
    }

    /**
     * A "stupid-backoff" smoothing model simialr to <a
     * href="http://en.wikipedia.org/wiki/Katz's_back-off_model"> Katz's
     * Backoff</a>. This model is used as the default if no model is configured.
     * <p>
     * See <a
     * href="http://en.wikipedia.org/wiki/N-gram#Smoothing_techniques">N-Gram
     * Smoothing</a> for details.
     * </p>
     */
    public static final class StupidBackoff extends SmoothingModel {
        private final double discount;

        /**
         * Creates a Stupid-Backoff smoothing model.
         * 
         * @param discount
         *            the discount given to lower order ngrams if the higher order ngram doesn't exits
         */
        public StupidBackoff(double discount) {
            super("stupid_backoff");
            this.discount = discount;
        }

        @Override
        protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("discount", discount);
            return builder;
        }
    }

    /**
     * An <a href="http://en.wikipedia.org/wiki/Additive_smoothing">additive
     * smoothing</a> model. 
     * <p>
     * See <a
     * href="http://en.wikipedia.org/wiki/N-gram#Smoothing_techniques">N-Gram
     * Smoothing</a> for details.
     * </p>
     */
    public static final class Laplace extends SmoothingModel {
        private final double alpha;
        /**
         * Creates a Laplace smoothing model.
         * 
         * @param discount
         *            the discount given to lower order ngrams if the higher order ngram doesn't exits
         */
        public Laplace(double alpha) {
            super("laplace");
            this.alpha = alpha;
        }

        @Override
        protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("alpha", alpha);
            return builder;
        }
    }
    
    
    public static abstract class SmoothingModel implements ToXContent {
        private final String type;

        protected SmoothingModel(String type) {
            this.type = type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(type);
            innerToXContent(builder,params);
            builder.endObject();
            return builder;
        }
        
        protected abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;
    }

    /**
     * Linear interpolation smoothing model.
     * <p>
     * See <a
     * href="http://en.wikipedia.org/wiki/N-gram#Smoothing_techniques">N-Gram
     * Smoothing</a> for details.
     * </p>
     */
    public static final class LinearInterpolation extends SmoothingModel {
        private final double trigramLambda;
        private final double bigramLambda;
        private final double unigramLambda;

        /**
         * Creates a linear interpolation smoothing model.
         * 
         * Note: the lambdas must sum up to one.
         * 
         * @param trigramLambda
         *            the trigram lambda
         * @param bigramLambda
         *            the bigram lambda
         * @param unigramLambda
         *            the unigram lambda
         */
        public LinearInterpolation(double trigramLambda, double bigramLambda, double unigramLambda) {
            super("linear");
            this.trigramLambda = trigramLambda;
            this.bigramLambda = bigramLambda;
            this.unigramLambda = unigramLambda;
        }

        @Override
        protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("trigram_lambda", trigramLambda);
            builder.field("bigram_lambda", bigramLambda);
            builder.field("unigram_lambda", unigramLambda);
            return builder;
        }
    }

    /**
     * {@link CandidateGenerator} base class. 
     */
    public static abstract class CandidateGenerator implements ToXContent {
        private final String type;

        public CandidateGenerator(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

    }

    /**
     * 
     *
     */
    public static final class DirectCandidateGenerator extends CandidateGenerator {
        private final String field;
        private String preFilter;
        private String postFilter;
        private String suggestMode;
        private Float accuracy;
        private Integer size;
        private String sort;
        private String stringDistance;
        private Integer maxEdits;
        private Integer maxInspections;
        private Float maxTermFreq;
        private Integer prefixLength;
        private Integer minWordLength;
        private Float minDocFreq;

        /**
         * Sets from what field to fetch the candidate suggestions from. This is
         * an required option and needs to be set via this setter or
         * {@link org.elasticsearch.search.suggest.SuggestBuilder.TermSuggestionBuilder#setField(String)}
         * method
         */
        public DirectCandidateGenerator(String field) {
            super("direct_generator");
            this.field = field;
        }

        /**
         * The global suggest mode controls what suggested terms are included or
         * controls for what suggest text tokens, terms should be suggested for.
         * Three possible values can be specified:
         * <ol>
         * <li><code>missing</code> - Only suggest terms in the suggest text
         * that aren't in the index. This is the default.
         * <li><code>popular</code> - Only suggest terms that occur in more docs
         * then the original suggest text term.
         * <li><code>always</code> - Suggest any matching suggest terms based on
         * tokens in the suggest text.
         * </ol>
         */
        public DirectCandidateGenerator suggestMode(String suggestMode) {
            this.suggestMode = suggestMode;
            return this;
        }

        /**
         * Sets how similar the suggested terms at least need to be compared to
         * the original suggest text tokens. A value between 0 and 1 can be
         * specified. This value will be compared to the string distance result
         * of each candidate spelling correction.
         * <p/>
         * Default is <tt>0.5</tt>
         */
        public DirectCandidateGenerator accuracy(float accuracy) {
            this.accuracy = accuracy;
            return this;
        }

        /**
         * Sets the maximum suggestions to be returned per suggest text term.
         */
        public DirectCandidateGenerator size(int size) {
            if (size <= 0) {
                throw new ElasticsearchIllegalArgumentException("Size must be positive");
            }
            this.size = size;
            return this;
        }

        /**
         * Sets how to sort the suggest terms per suggest text token. Two
         * possible values:
         * <ol>
         * <li><code>score</code> - Sort should first be based on score, then
         * document frequency and then the term itself.
         * <li><code>frequency</code> - Sort should first be based on document
         * frequency, then scotr and then the term itself.
         * </ol>
         * <p/>
         * What the score is depends on the suggester being used.
         */
        public DirectCandidateGenerator sort(String sort) {
            this.sort = sort;
            return this;
        }

        /**
         * Sets what string distance implementation to use for comparing how
         * similar suggested terms are. Four possible values can be specified:
         * <ol>
         * <li><code>internal</code> - This is the default and is based on
         * <code>damerau_levenshtein</code>, but highly optimized for comparing
         * string distance for terms inside the index.
         * <li><code>damerau_levenshtein</code> - String distance algorithm
         * based on Damerau-Levenshtein algorithm.
         * <li><code>levenstein</code> - String distance algorithm based on
         * Levenstein edit distance algorithm.
         * <li><code>jarowinkler</code> - String distance algorithm based on
         * Jaro-Winkler algorithm.
         * <li><code>ngram</code> - String distance algorithm based on character
         * n-grams.
         * </ol>
         */
        public DirectCandidateGenerator stringDistance(String stringDistance) {
            this.stringDistance = stringDistance;
            return this;
        }

        /**
         * Sets the maximum edit distance candidate suggestions can have in
         * order to be considered as a suggestion. Can only be a value between 1
         * and 2. Any other value result in an bad request error being thrown.
         * Defaults to <tt>2</tt>.
         */
        public DirectCandidateGenerator maxEdits(Integer maxEdits) {
            this.maxEdits = maxEdits;
            return this;
        }

        /**
         * A factor that is used to multiply with the size in order to inspect
         * more candidate suggestions. Can improve accuracy at the cost of
         * performance. Defaults to <tt>5</tt>.
         */
        public DirectCandidateGenerator maxInspections(Integer maxInspections) {
            this.maxInspections = maxInspections;
            return this;
        }

        /**
         * Sets a maximum threshold in number of documents a suggest text token
         * can exist in order to be corrected. Can be a relative percentage
         * number (e.g 0.4) or an absolute number to represent document
         * frequencies. If an value higher than 1 is specified then fractional
         * can not be specified. Defaults to <tt>0.01</tt>.
         * <p/>
         * This can be used to exclude high frequency terms from being
         * suggested. High frequency terms are usually spelled correctly on top
         * of this this also improves the suggest performance.
         */
        public DirectCandidateGenerator maxTermFreq(float maxTermFreq) {
            this.maxTermFreq = maxTermFreq;
            return this;
        }

        /**
         * Sets the number of minimal prefix characters that must match in order
         * be a candidate suggestion. Defaults to 1. Increasing this number
         * improves suggest performance. Usually misspellings don't occur in the
         * beginning of terms.
         */
        public DirectCandidateGenerator prefixLength(int prefixLength) {
            this.prefixLength = prefixLength;
            return this;
        }

        /**
         * The minimum length a suggest text term must have in order to be
         * corrected. Defaults to <tt>4</tt>.
         */
        public DirectCandidateGenerator minWordLength(int minWordLength) {
            this.minWordLength = minWordLength;
            return this;
        }

        /**
         * Sets a minimal threshold in number of documents a suggested term
         * should appear in. This can be specified as an absolute number or as a
         * relative percentage of number of documents. This can improve quality
         * by only suggesting high frequency terms. Defaults to 0f and is not
         * enabled. If a value higher than 1 is specified then the number cannot
         * be fractional.
         */
        public DirectCandidateGenerator minDocFreq(float minDocFreq) {
            this.minDocFreq = minDocFreq;
            return this;
        }

        /**
         * Sets a filter (analyzer) that is applied to each of the tokens passed to this candidate generator.
         * This filter is applied to the original token before candidates are generated.
         */
        public DirectCandidateGenerator preFilter(String preFilter) {
            this.preFilter = preFilter;
            return this;
        }

        /**
         * Sets a filter (analyzer) that is applied to each of the generated tokens
         * before they are passed to the actual phrase scorer.
         */
        public DirectCandidateGenerator postFilter(String postFilter) {
            this.postFilter = postFilter;
            return this;
        }
        
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
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
            if (maxEdits != null) {
                builder.field("max_edits", maxEdits);
            }
            if (maxInspections != null) {
                builder.field("max_inspections", maxInspections);
            }
            if (maxTermFreq != null) {
                builder.field("max_term_freq", maxTermFreq);
            }
            if (prefixLength != null) {
                builder.field("prefix_length", prefixLength);
            }
            if (minWordLength != null) {
                builder.field("min_word_length", minWordLength);
            }
            if (minDocFreq != null) {
                builder.field("min_doc_freq", minDocFreq);
            }
            if (preFilter != null) {
                builder.field("pre_filter", preFilter);
            }
            if (postFilter != null) {
                builder.field("post_filter", postFilter);
            }
            builder.endObject();
            return builder;
        }

    }

}
