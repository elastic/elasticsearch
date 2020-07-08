/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class FeatureImportance implements Writeable, ToXContentObject {

    private final Map<String, Double> classImportance;
    private final double importance;
    private final String featureName;
    static final String IMPORTANCE = "importance";
    static final String FEATURE_NAME = "feature_name";
    static final String CLASS_IMPORTANCE = "class_importance";

    public static FeatureImportance forRegression(String featureName, double importance) {
        return new FeatureImportance(featureName, importance, null);
    }

    public static FeatureImportance forClassification(String featureName, Map<String, Double> classImportance) {
        return new FeatureImportance(featureName, classImportance.values().stream().mapToDouble(Math::abs).sum(), classImportance);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FeatureImportance, Void> PARSER =
        new ConstructingObjectParser<>("feature_importance",
            a -> new FeatureImportance((String) a[0], (Double) a[1], (Map<String, Double>) a[2])
        );

    static {
        PARSER.declareString(constructorArg(), new ParseField(FeatureImportance.FEATURE_NAME));
        PARSER.declareDouble(constructorArg(), new ParseField(FeatureImportance.IMPORTANCE));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(HashMap::new, XContentParser::doubleValue),
            new ParseField(FeatureImportance.CLASS_IMPORTANCE));
    }

    public static FeatureImportance fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    FeatureImportance(String featureName, double importance, Map<String, Double> classImportance) {
        this.featureName = Objects.requireNonNull(featureName);
        this.importance = importance;
        this.classImportance = classImportance == null ? null : Collections.unmodifiableMap(classImportance);
    }

    public FeatureImportance(StreamInput in) throws IOException {
        this.featureName = in.readString();
        this.importance = in.readDouble();
        if (in.readBoolean()) {
            this.classImportance = in.readMap(StreamInput::readString, StreamInput::readDouble);
        } else {
            this.classImportance = null;
        }
    }

    public Map<String, Double> getClassImportance() {
        return classImportance;
    }

    public double getImportance() {
        return importance;
    }

    public String getFeatureName() {
        return featureName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.featureName);
        out.writeDouble(this.importance);
        out.writeBoolean(this.classImportance != null);
        if (this.classImportance != null) {
            out.writeMap(this.classImportance, StreamOutput::writeString, StreamOutput::writeDouble);
        }
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(FEATURE_NAME, featureName);
        map.put(IMPORTANCE, importance);
        if (classImportance != null) {
            classImportance.forEach(map::put);
        }
        return map;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FEATURE_NAME, featureName);
        builder.field(IMPORTANCE, importance);
        if (classImportance != null && classImportance.isEmpty() == false) {
            builder.startObject(CLASS_IMPORTANCE);
            for (Map.Entry<String, Double> entry : classImportance.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        FeatureImportance that = (FeatureImportance) object;
        return Objects.equals(featureName, that.featureName)
            && Objects.equals(importance, that.importance)
            && Objects.equals(classImportance, that.classImportance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureName, importance, classImportance);
    }
}
