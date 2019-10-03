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

public class ClassificationInferenceResults extends SingleValueInferenceResults {

    public static final String RESULT_TYPE = "classification";
    public static final ParseField CLASSIFICATION_LABEL = new ParseField("classification_label");
    public static final ParseField TOP_CLASSES = new ParseField("top_classes");
    
    private final String classificationLabel;
    private final List<TopClassEntry> topClasses;

    public ClassificationInferenceResults(double value, String classificationLabel, List<TopClassEntry> topClasses) {
        super(value);
        this.classificationLabel = classificationLabel;
        this.topClasses = topClasses == null ? null : Collections.unmodifiableList(topClasses);
    }

    public ClassificationInferenceResults(StreamInput in) throws IOException {
        super(in);
        this.classificationLabel = in.readOptionalString();
        if (in.readBoolean()) {
            this.topClasses = Collections.unmodifiableList(in.readList(TopClassEntry::new));
        } else {
            this.topClasses = null;
        }
    }

    public String getClassificationLabel() {
        return classificationLabel;
    }

    public List<TopClassEntry> getTopClasses() {
        return topClasses;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(classificationLabel);
        out.writeBoolean(topClasses != null);
        if (topClasses != null) {
            out.writeCollection(topClasses);
        }
    }

    @Override
    XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (classificationLabel != null) {
            builder.field(CLASSIFICATION_LABEL.getPreferredName(), classificationLabel);
        }
        if (topClasses != null) {
            builder.field(TOP_CLASSES.getPreferredName(), topClasses);
        }
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        ClassificationInferenceResults that = (ClassificationInferenceResults) object;
        return Objects.equals(value(), that.value()) &&
            Objects.equals(classificationLabel, that.classificationLabel) &&
            Objects.equals(topClasses, that.topClasses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value(), classificationLabel, topClasses);
    }

    @Override
    public String resultType() {
        return RESULT_TYPE;
    }

    @Override
    public String valueAsString() {
        return classificationLabel == null ? super.valueAsString() : classificationLabel;
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
