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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.suggest.phrase.WordScorer.WordScorerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Linear interpolation smoothing model.
 * <p>
 * See <a
 * href="http://en.wikipedia.org/wiki/N-gram#Smoothing_techniques">N-Gram
 * Smoothing</a> for details.
 * </p>
 */
public final class LinearInterpolation extends SmoothingModel {
    public static final String NAME = "linear";
    static final ParseField PARSE_FIELD = new ParseField(NAME);
    private static final ParseField TRIGRAM_FIELD = new ParseField("trigram_lambda");
    private static final ParseField BIGRAM_FIELD = new ParseField("bigram_lambda");
    private static final ParseField UNIGRAM_FIELD = new ParseField("unigram_lambda");

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
        double sum = trigramLambda + bigramLambda + unigramLambda;
        if (Math.abs(sum - 1.0) > 0.001) {
            throw new IllegalArgumentException("linear smoothing lambdas must sum to 1");
        }
        this.trigramLambda = trigramLambda;
        this.bigramLambda = bigramLambda;
        this.unigramLambda = unigramLambda;
    }

    /**
     * Read from a stream.
     */
    public LinearInterpolation(StreamInput in) throws IOException {
        trigramLambda = in.readDouble();
        bigramLambda = in.readDouble();
        unigramLambda = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(trigramLambda);
        out.writeDouble(bigramLambda);
        out.writeDouble(unigramLambda);
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
    protected boolean doEquals(SmoothingModel other) {
        final LinearInterpolation otherModel = (LinearInterpolation) other;
        return Objects.equals(trigramLambda, otherModel.trigramLambda) &&
                Objects.equals(bigramLambda, otherModel.bigramLambda) &&
                Objects.equals(unigramLambda, otherModel.unigramLambda);
    }

    @Override
    protected final int doHashCode() {
        return Objects.hash(trigramLambda, bigramLambda, unigramLambda);
    }

    public static LinearInterpolation innerFromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        XContentParser.Token token;
        String fieldName = null;
        double trigramLambda = 0.0;
        double bigramLambda = 0.0;
        double unigramLambda = 0.0;
        ParseFieldMatcher matcher = parseContext.getParseFieldMatcher();
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
                throw new ParsingException(parser.getTokenLocation(),
                        "[" + NAME + "] unknown token [" + token + "] after [" + fieldName + "]");
            }
        }
        return new LinearInterpolation(trigramLambda, bigramLambda, unigramLambda);
    }

    @Override
    public WordScorerFactory buildWordScorerFactory() {
        return (IndexReader reader, Terms terms, String field, double realWordLikelyhood, BytesRef separator) ->
                    new LinearInterpolatingScorer(reader, terms, field, realWordLikelyhood, separator, trigramLambda, bigramLambda,
                        unigramLambda);
    }
}