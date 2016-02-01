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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Template;
import org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.WordScorer.WordScorerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

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
    private Template collateQuery;
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
            throw new IllegalArgumentException("gramSize must be >= 1");
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
     * {@link PhraseSuggestionBuilder.StupidBackoff}.
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
            throw new IllegalArgumentException("Pre and post tag must both be null or both not be null.");
        }
        this.preTag = preTag;
        this.postTag = postTag;
        return this;
    }

    /**
     * Sets a query used for filtering out suggested phrases (collation).
     */
    public PhraseSuggestionBuilder collateQuery(String collateQuery) {
        this.collateQuery = new Template(collateQuery);
        return this;
    }

    /**
     * Sets a query used for filtering out suggested phrases (collation).
     */
    public PhraseSuggestionBuilder collateQuery(Template collateQueryTemplate) {
        this.collateQuery = collateQueryTemplate;
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
        if (collateQuery != null) {
            builder.startObject("collate");
            builder.field("query", collateQuery);
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
     * Creates a new {@link DirectCandidateGeneratorBuilder}
     *
     * @param field
     *            the field this candidate generator operates on.
     */
    public static DirectCandidateGeneratorBuilder candidateGenerator(String field) {
        return new DirectCandidateGeneratorBuilder(field);
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
        /**
         * Default discount parameter for {@link StupidBackoff} smoothing
         */
        public static final double DEFAULT_BACKOFF_DISCOUNT = 0.4;
        private double discount = DEFAULT_BACKOFF_DISCOUNT;
        static final StupidBackoff PROTOTYPE = new StupidBackoff(DEFAULT_BACKOFF_DISCOUNT);
        private static final String NAME = "stupid_backoff";
        private static final ParseField DISCOUNT_FIELD = new ParseField("discount");

        /**
         * Creates a Stupid-Backoff smoothing model.
         *
         * @param discount
         *            the discount given to lower order ngrams if the higher order ngram doesn't exits
         */
        public StupidBackoff(double discount) {
            this.discount = discount;
        }

        /**
         * @return the discount parameter of the model
         */
        public double getDiscount() {
            return this.discount;
        }

        @Override
        protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(DISCOUNT_FIELD.getPreferredName(), discount);
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(discount);
        }

        @Override
        public StupidBackoff readFrom(StreamInput in) throws IOException {
            return new StupidBackoff(in.readDouble());
        }

        @Override
        protected boolean doEquals(SmoothingModel other) {
            StupidBackoff otherModel = (StupidBackoff) other;
            return Objects.equals(discount, otherModel.discount);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(discount);
        }

        @Override
        public SmoothingModel fromXContent(QueryParseContext parseContext) throws IOException {
            XContentParser parser = parseContext.parser();
            XContentParser.Token token;
            String fieldName = null;
            double discount = DEFAULT_BACKOFF_DISCOUNT;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                }
                if (token.isValue() && parseContext.parseFieldMatcher().match(fieldName, DISCOUNT_FIELD)) {
                    discount = parser.doubleValue();
                }
            }
            return new StupidBackoff(discount);
        }

        @Override
        public WordScorerFactory buildWordScorerFactory() {
            return (IndexReader reader, Terms terms, String field, double realWordLikelyhood, BytesRef separator)
                    -> new StupidBackoffScorer(reader, terms, field, realWordLikelyhood, separator, discount);
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
        private double alpha = DEFAULT_LAPLACE_ALPHA;
        private static final String NAME = "laplace";
        private static final ParseField ALPHA_FIELD = new ParseField("alpha");
        /**
         * Default alpha parameter for laplace smoothing
         */
        public static final double DEFAULT_LAPLACE_ALPHA = 0.5;
        static final Laplace PROTOTYPE = new Laplace(DEFAULT_LAPLACE_ALPHA);

        /**
         * Creates a Laplace smoothing model.
         *
         */
        public Laplace(double alpha) {
            this.alpha = alpha;
        }

        /**
         * @return the laplace model alpha parameter
         */
        public double getAlpha() {
            return this.alpha;
        }

        @Override
        protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(ALPHA_FIELD.getPreferredName(), alpha);
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(alpha);
        }

        @Override
        public SmoothingModel readFrom(StreamInput in) throws IOException {
            return new Laplace(in.readDouble());
        }

        @Override
        protected boolean doEquals(SmoothingModel other) {
            Laplace otherModel = (Laplace) other;
            return Objects.equals(alpha, otherModel.alpha);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(alpha);
        }

        @Override
        public SmoothingModel fromXContent(QueryParseContext parseContext) throws IOException {
            XContentParser parser = parseContext.parser();
            XContentParser.Token token;
            String fieldName = null;
            double alpha = DEFAULT_LAPLACE_ALPHA;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                }
                if (token.isValue() && parseContext.parseFieldMatcher().match(fieldName, ALPHA_FIELD)) {
                    alpha = parser.doubleValue();
                }
            }
            return new Laplace(alpha);
        }

        @Override
        public WordScorerFactory buildWordScorerFactory() {
            return (IndexReader reader, Terms terms, String field, double realWordLikelyhood, BytesRef separator)
                    -> new LaplaceScorer(reader, terms,  field, realWordLikelyhood, separator, alpha);
        }
    }


    public static abstract class SmoothingModel implements NamedWriteable<SmoothingModel>, ToXContent {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(getWriteableName());
            innerToXContent(builder,params);
            builder.endObject();
            return builder;
        }

        @Override
        public final boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            @SuppressWarnings("unchecked")
            SmoothingModel other = (SmoothingModel) obj;
            return doEquals(other);
        }

        public abstract SmoothingModel fromXContent(QueryParseContext parseContext) throws IOException;

        public abstract WordScorerFactory buildWordScorerFactory();

        /**
         * subtype specific implementation of "equals".
         */
        protected abstract boolean doEquals(SmoothingModel other);

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
        private static final String NAME = "linear";
        static final LinearInterpolation PROTOTYPE = new LinearInterpolation(0.8, 0.1, 0.1);
        private final double trigramLambda;
        private final double bigramLambda;
        private final double unigramLambda;
        private static final ParseField TRIGRAM_FIELD = new ParseField("trigram_lambda");
        private static final ParseField BIGRAM_FIELD = new ParseField("bigram_lambda");
        private static final ParseField UNIGRAM_FIELD = new ParseField("unigram_lambda");

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
            double sum = trigramLambda + bigramLambda + unigramLambda;
            if (Math.abs(sum - 1.0) > 0.001) {
                throw new IllegalArgumentException("linear smoothing lambdas must sum to 1");
            }
            this.trigramLambda = trigramLambda;
            this.bigramLambda = bigramLambda;
            this.unigramLambda = unigramLambda;
        }

        public double getTrigramLambda() {
            return this.trigramLambda;
        }

        public double getBigramLambda() {
            return this.bigramLambda;
        }

        public double getUnigramLambda() {
            return this.unigramLambda;
        }

        @Override
        protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(TRIGRAM_FIELD.getPreferredName(), trigramLambda);
            builder.field(BIGRAM_FIELD.getPreferredName(), bigramLambda);
            builder.field(UNIGRAM_FIELD.getPreferredName(), unigramLambda);
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(trigramLambda);
            out.writeDouble(bigramLambda);
            out.writeDouble(unigramLambda);
        }

        @Override
        public LinearInterpolation readFrom(StreamInput in) throws IOException {
            return new LinearInterpolation(in.readDouble(), in.readDouble(), in.readDouble());
        }

        @Override
        protected boolean doEquals(SmoothingModel other) {
            final LinearInterpolation otherModel = (LinearInterpolation) other;
            return Objects.equals(trigramLambda, otherModel.trigramLambda) &&
                    Objects.equals(bigramLambda, otherModel.bigramLambda) &&
                    Objects.equals(unigramLambda, otherModel.unigramLambda);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(trigramLambda, bigramLambda, unigramLambda);
        }

        @Override
        public LinearInterpolation fromXContent(QueryParseContext parseContext) throws IOException {
            XContentParser parser = parseContext.parser();
            XContentParser.Token token;
            String fieldName = null;
            double trigramLambda = 0.0;
            double bigramLambda = 0.0;
            double unigramLambda = 0.0;
            ParseFieldMatcher matcher = parseContext.parseFieldMatcher();
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (matcher.match(fieldName, TRIGRAM_FIELD)) {
                        trigramLambda = parser.doubleValue();
                        if (trigramLambda < 0) {
                            throw new IllegalArgumentException("trigram_lambda must be positive");
                        }
                    } else if (matcher.match(fieldName, BIGRAM_FIELD)) {
                        bigramLambda = parser.doubleValue();
                        if (bigramLambda < 0) {
                            throw new IllegalArgumentException("bigram_lambda must be positive");
                        }
                    } else if (matcher.match(fieldName, UNIGRAM_FIELD)) {
                        unigramLambda = parser.doubleValue();
                        if (unigramLambda < 0) {
                            throw new IllegalArgumentException("unigram_lambda must be positive");
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "suggester[phrase][smoothing][linear] doesn't support field [" + fieldName + "]");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown token [" + token + "] after [" + fieldName + "]");
                }
            }
            return new LinearInterpolation(trigramLambda, bigramLambda, unigramLambda);
        }

        @Override
        public WordScorerFactory buildWordScorerFactory() {
            return (IndexReader reader, Terms terms, String field, double realWordLikelyhood, BytesRef separator) ->
                        new LinearInterpoatingScorer(reader, terms, field, realWordLikelyhood, separator, trigramLambda, bigramLambda,
                            unigramLambda);
        }
    }

    /**
     * {@link CandidateGenerator} interface.
     */
    public interface CandidateGenerator extends ToXContent {
        String getType();

        CandidateGenerator fromXContent(QueryParseContext parseContext) throws IOException;
    }
}
