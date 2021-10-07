/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class TopClassEntry implements Writeable, ToXContentObject {

    public static final ParseField CLASS_NAME = new ParseField("class_name");
    public static final ParseField CLASS_PROBABILITY = new ParseField("class_probability");
    public static final ParseField CLASS_SCORE = new ParseField("class_score");

    public static final String NAME = "top_class";

    private static final ConstructingObjectParser<TopClassEntry, Void> PARSER =
        new ConstructingObjectParser<>(NAME, a -> new TopClassEntry(a[0], (Double) a[1], (Double) a[2]));

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

    public TopClassEntry(Object classification, double score) {
        this(classification, score, score);
    }

    public TopClassEntry(Object classification, double probability, double score) {
        this.classification = ExceptionsHelper.requireNonNull(classification, CLASS_NAME);
        this.probability = probability;
        this.score = score;
    }

    public TopClassEntry(StreamInput in) throws IOException {
        this.classification = in.readGenericValue();
        this.probability = in.readDouble();
        this.score = in.readDouble();
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

    public Map<String, Object> asValueMap() {
        Map<String, Object> map = new HashMap<>(3, 1.0f);
        map.put(CLASS_NAME.getPreferredName(), classification);
        map.put(CLASS_PROBABILITY.getPreferredName(), probability);
        map.put(CLASS_SCORE.getPreferredName(), score);
        return map;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLASS_NAME.getPreferredName(), classification);
        builder.field(CLASS_PROBABILITY.getPreferredName(), probability);
        builder.field(CLASS_SCORE.getPreferredName(), score);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(classification);
        out.writeDouble(probability);
        out.writeDouble(score);
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
