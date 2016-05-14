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
 * An <a href="http://en.wikipedia.org/wiki/Additive_smoothing">additive
 * smoothing</a> model.
 * <p>
 * See <a
 * href="http://en.wikipedia.org/wiki/N-gram#Smoothing_techniques">N-Gram
 * Smoothing</a> for details.
 * </p>
 */
public final class Laplace extends SmoothingModel {
    public static final String NAME = "laplace";
    private static final ParseField ALPHA_FIELD = new ParseField("alpha");
    static final ParseField PARSE_FIELD = new ParseField(NAME);
    /**
     * Default alpha parameter for laplace smoothing
     */
    public static final double DEFAULT_LAPLACE_ALPHA = 0.5;

    private double alpha = DEFAULT_LAPLACE_ALPHA;

    /**
     * Creates a Laplace smoothing model.
     *
     */
    public Laplace(double alpha) {
        this.alpha = alpha;
    }

    /**
     * Read from a stream.
     */
    public Laplace(StreamInput in) throws IOException {
        alpha = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(alpha);
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
    protected boolean doEquals(SmoothingModel other) {
        Laplace otherModel = (Laplace) other;
        return Objects.equals(alpha, otherModel.alpha);
    }

    @Override
    protected final int doHashCode() {
        return Objects.hash(alpha);
    }

    public static SmoothingModel innerFromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        XContentParser.Token token;
        String fieldName = null;
        double alpha = DEFAULT_LAPLACE_ALPHA;
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            }
            if (token.isValue() && parseContext.getParseFieldMatcher().match(fieldName, ALPHA_FIELD)) {
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