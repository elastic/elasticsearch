/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class Hyperparameters implements ToXContentObject, Writeable {

    private static final String NAME = "hyperparameters";
    public static final ParseField HYPERPARAMETER_NAME = new ParseField("name");
    public static final ParseField VALUE = new ParseField("value");
    public static final ParseField ABSOLUTE_IMPORTANCE = new ParseField("absolute_importance");
    public static final ParseField RELATIVE_IMPORTANCE = new ParseField("relative_importance");
    public static final ParseField SUPPLIED = new ParseField("supplied");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<Hyperparameters, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<Hyperparameters, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<Hyperparameters, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Hyperparameters, Void> parser = new ConstructingObjectParser<>(
            NAME,
            ignoreUnknownFields,
            a -> new Hyperparameters((String) a[0], (Double) a[1], (Double) a[2], (Double) a[3], (Boolean) a[4])
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), HYPERPARAMETER_NAME);
        parser.declareDouble(ConstructingObjectParser.constructorArg(), VALUE);
        parser.declareDouble(ConstructingObjectParser.optionalConstructorArg(), ABSOLUTE_IMPORTANCE);
        parser.declareDouble(ConstructingObjectParser.optionalConstructorArg(), RELATIVE_IMPORTANCE);
        parser.declareBoolean(ConstructingObjectParser.constructorArg(), SUPPLIED);
        return parser;
    }

    public static Hyperparameters fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    public final String hyperparameterName;
    public final double value;
    public final Double absoluteImportance;
    public final Double relativeImportance;
    public final boolean supplied;

    public Hyperparameters(StreamInput in) throws IOException {
        this.hyperparameterName = in.readString();
        this.value = in.readDouble();
        this.supplied = in.readBoolean();
        this.absoluteImportance = in.readOptionalDouble();
        this.relativeImportance = in.readOptionalDouble();
    }

    Hyperparameters(String hyperparameterName, double value, Double absoluteImportance, Double relativeImportance, boolean supplied) {
        this.hyperparameterName = hyperparameterName;
        this.value = value;
        this.supplied = supplied;
        this.absoluteImportance = absoluteImportance;
        this.relativeImportance = relativeImportance;
        if (this.supplied == false) {
            ExceptionsHelper.requireNonNull(absoluteImportance, ABSOLUTE_IMPORTANCE.getPreferredName());
            ExceptionsHelper.requireNonNull(relativeImportance, RELATIVE_IMPORTANCE.getPreferredName());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(hyperparameterName);
        out.writeDouble(value);
        out.writeBoolean(supplied);
        out.writeOptionalDouble(absoluteImportance);
        out.writeOptionalDouble(relativeImportance);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(asMap());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Hyperparameters that = (Hyperparameters) o;
        return Objects.equals(that.hyperparameterName, hyperparameterName)
            && Objects.equals(value, that.value)
            && Objects.equals(absoluteImportance, that.absoluteImportance)
            && Objects.equals(relativeImportance, that.relativeImportance)
            && Objects.equals(supplied, that.supplied);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(HYPERPARAMETER_NAME.getPreferredName(), hyperparameterName);
        map.put(VALUE.getPreferredName(), value);
        if (absoluteImportance != null) {
            map.put(ABSOLUTE_IMPORTANCE.getPreferredName(), absoluteImportance);
        }
        if (relativeImportance != null) {
            map.put(RELATIVE_IMPORTANCE.getPreferredName(), relativeImportance);
        }
        map.put(SUPPLIED.getPreferredName(), supplied);

        return map;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hyperparameterName, value, absoluteImportance, relativeImportance, supplied);
    }
}
