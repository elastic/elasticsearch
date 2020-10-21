/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml.inference.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class TopClassEntry implements ToXContentObject {

    public static final ParseField CLASS_NAME = new ParseField("class_name");
    public static final ParseField CLASS_PROBABILITY = new ParseField("class_probability");
    public static final ParseField CLASS_SCORE = new ParseField("class_score");

    public static final String NAME = "top_class";

    private static final ConstructingObjectParser<TopClassEntry, Void> PARSER =
        new ConstructingObjectParser<>(NAME, true, a -> new TopClassEntry(a[0], (Double) a[1], (Double) a[2]));

    static {
        PARSER.declareField(constructorArg(), (p, n) -> {
            Object o;
            XContentParser.Token token = p.currentToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                o = p.text();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                o = p.booleanValue();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                o = p.doubleValue();
            } else {
                throw new XContentParseException(p.getTokenLocation(),
                    "[" + NAME + "] failed to parse field [" + CLASS_NAME + "] value [" + token
                        + "] is not a string, boolean or number");
            }
            return o;
        }, CLASS_NAME, ObjectParser.ValueType.VALUE);
        PARSER.declareDouble(constructorArg(), CLASS_PROBABILITY);
        PARSER.declareDouble(constructorArg(), CLASS_SCORE);
    }

    public static TopClassEntry fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Object classification;
    private final double probability;
    private final double score;

    public TopClassEntry(Object classification, double probability, double score) {
        this.classification = Objects.requireNonNull(classification);
        this.probability = probability;
        this.score = score;
    }

    public Object getClassification() {
        return classification;
    }

    public double getProbability() {
        return probability;
    }

    public double getScore() {
        return score;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(CLASS_NAME.getPreferredName(), classification);
        builder.field(CLASS_PROBABILITY.getPreferredName(), probability);
        builder.field(CLASS_SCORE.getPreferredName(), score);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        TopClassEntry that = (TopClassEntry) object;
        return Objects.equals(classification, that.classification) && probability == that.probability && score == that.score;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classification, probability, score);
    }
}
