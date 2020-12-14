/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class HyperparameterImportance implements ToXContentObject, Writeable {

    private static final String NAME = "hyperparameter_importance";
    public static final ParseField HYPERPARAMETER_NAME = new ParseField("name");
    public static final ParseField VALUE = new ParseField("value");
    public static final ParseField ABSOLUTE_IMPORTANCE = new ParseField("absolute_importance");
    public static final ParseField RELATIVE_IMPORTANCE = new ParseField("relative_importance");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<HyperparameterImportance, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<HyperparameterImportance, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<HyperparameterImportance, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<HyperparameterImportance, Void> parser = new ConstructingObjectParser<>(NAME,
            ignoreUnknownFields,
            a -> new HyperparameterImportance((String)a[0], (Double)a[1], (Double)a[2], (Double)a[3]));
        parser.declareString(ConstructingObjectParser.constructorArg(), HYPERPARAMETER_NAME);
        parser.declareDouble(ConstructingObjectParser.constructorArg(), VALUE);
        parser.declareDouble(ConstructingObjectParser.constructorArg(), ABSOLUTE_IMPORTANCE);
        parser.declareDouble(ConstructingObjectParser.constructorArg(), RELATIVE_IMPORTANCE);
        return parser;
    }

    public static HyperparameterImportance fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    public final String hyperparameterName;
    public final Double value;
    public final Double absoluteImportance;
    public final Double relativeImportance;

    public HyperparameterImportance(StreamInput in) throws IOException {
        this.hyperparameterName = in.readString();
        this.value = in.readDouble();
        this.absoluteImportance = in.readDouble();
        this.relativeImportance = in.readDouble();
    }

    HyperparameterImportance(String hyperparameterName, Double value, Double absoluteImportance, Double relativeImportance) {
        this.hyperparameterName = hyperparameterName;
        this.value = value;
        this.absoluteImportance = absoluteImportance;
        this.relativeImportance = relativeImportance;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(hyperparameterName);
        out.writeDouble(value);
        out.writeDouble(absoluteImportance);
        out.writeDouble(relativeImportance);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(asMap());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HyperparameterImportance that = (HyperparameterImportance) o;
        return Objects.equals(that.hyperparameterName, hyperparameterName)
            && Objects.equals(value, that.value)
            && Objects.equals(absoluteImportance, that.absoluteImportance)
            && Objects.equals(relativeImportance, that.relativeImportance);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(HYPERPARAMETER_NAME.getPreferredName(), hyperparameterName);
        map.put(VALUE.getPreferredName(), value);
        map.put(ABSOLUTE_IMPORTANCE.getPreferredName(), absoluteImportance);
        map.put(RELATIVE_IMPORTANCE.getPreferredName(), relativeImportance);
        
        return map;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hyperparameterName, value, absoluteImportance, relativeImportance);
    }
}
