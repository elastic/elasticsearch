/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

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
