/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class InferenceResults implements ToXContentObject, Writeable {

    public final ParseField TOP_CLASSES = new ParseField("top_classes");
    public final ParseField NUMERIC_VALUE = new ParseField("numeric_value");
    public final ParseField CLASSIFICATION_LABEL = new ParseField("classification_label");

    private final double numericValue;
    private final String classificationLabel;
    private final List<TopClassEntry> topClasses;

    public static InferenceResults valueOnly(double value) {
        return new InferenceResults(value, null, null);
    }

    public static InferenceResults valueAndLabel(double value, String classificationLabel) {
        return new InferenceResults(value, classificationLabel, null);
    }

    public InferenceResults(Double numericValue, String classificationLabel, List<TopClassEntry> topClasses) {
        this.numericValue = ExceptionsHelper.requireNonNull(numericValue, NUMERIC_VALUE);
        this.classificationLabel = classificationLabel;
        this.topClasses = topClasses == null ? null : Collections.unmodifiableList(topClasses);
    }

    public InferenceResults(StreamInput in) throws IOException {
        this.numericValue = in.readDouble();
        this.classificationLabel = in.readOptionalString();
        if (in.readBoolean()) {
            this.topClasses = Collections.unmodifiableList(in.readList(TopClassEntry::new));
        } else {
            this.topClasses = null;
        }
    }

    public double getNumericValue() {
        return numericValue;
    }

    public String getClassificationLabel() {
        return classificationLabel;
    }

    public List<TopClassEntry> getTopClasses() {
        return topClasses;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(numericValue);
        out.writeOptionalString(classificationLabel);
        out.writeBoolean(topClasses != null);
        if (topClasses != null) {
            out.writeList(topClasses);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUMERIC_VALUE.getPreferredName(), numericValue);
        if (classificationLabel != null) {
            builder.field(CLASSIFICATION_LABEL.getPreferredName(), classificationLabel);
        }
        if (topClasses != null) {
            builder.field(TOP_CLASSES.getPreferredName(), topClasses);
        }
        builder.endObject();
        return null;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        InferenceResults that = (InferenceResults) object;
        return Objects.equals(numericValue, that.numericValue) &&
            Objects.equals(classificationLabel, that.classificationLabel) &&
            Objects.equals(topClasses, that.topClasses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numericValue, classificationLabel, topClasses);
    }

    public static class TopClassEntry implements ToXContentObject, Writeable {

        public final ParseField LABEL = new ParseField("label");
        public final ParseField PROBABILITY = new ParseField("probability");

        private final String label;
        private final double probability;

        public TopClassEntry(String label, Double probability) {
            this.label = ExceptionsHelper.requireNonNull(label, LABEL);
            this.probability = ExceptionsHelper.requireNonNull(probability, PROBABILITY);
        }

        public TopClassEntry(StreamInput in) throws IOException {
            this.label = in.readString();
            this.probability = in.readDouble();
        }

        public String getLabel() {
            return label;
        }

        public double getProbability() {
            return probability;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(label);
            out.writeDouble(probability);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(LABEL.getPreferredName(), label);
            builder.field(PROBABILITY.getPreferredName(), probability);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object object) {
            if (object == this) { return true; }
            if (object == null || getClass() != object.getClass()) { return false; }
            TopClassEntry that = (TopClassEntry) object;
            return Objects.equals(label, that.label) &&
                Objects.equals(probability, that.probability);
        }

        @Override
        public int hashCode() {
            return Objects.hash(label, probability);
        }
    }
}
