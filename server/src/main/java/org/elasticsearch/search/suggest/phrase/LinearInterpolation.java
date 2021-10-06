/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
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
    protected int doHashCode() {
        return Objects.hash(trigramLambda, bigramLambda, unigramLambda);
    }

    public static LinearInterpolation fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String fieldName = null;
        double trigramLambda = 0.0;
        double bigramLambda = 0.0;
        double unigramLambda = 0.0;
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (TRIGRAM_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    trigramLambda = parser.doubleValue();
                    if (trigramLambda < 0) {
                        throw new IllegalArgumentException("trigram_lambda must be positive");
                    }
                } else if (BIGRAM_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    bigramLambda = parser.doubleValue();
                    if (bigramLambda < 0) {
                        throw new IllegalArgumentException("bigram_lambda must be positive");
                    }
                } else if (UNIGRAM_FIELD.match(fieldName, parser.getDeprecationHandler())) {
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
        return (IndexReader reader, Terms terms, String field, double realWordLikelihood, BytesRef separator) ->
                    new LinearInterpolatingScorer(reader, terms, field, realWordLikelihood, separator, trigramLambda, bigramLambda,
                        unigramLambda);
    }
}
