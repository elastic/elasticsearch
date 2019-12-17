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
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ClassificationInferenceResults extends SingleValueInferenceResults {

    public static final String NAME = "classification";

    private final String topNumClassesField;
    private final String resultsField;
    private final String classificationLabel;
    private final List<TopClassEntry> topClasses;

    public ClassificationInferenceResults(double value,
                                          String classificationLabel,
                                          List<TopClassEntry> topClasses,
                                          InferenceConfig config) {
        super(value);
        assert config instanceof ClassificationConfig;
        ClassificationConfig classificationConfig = (ClassificationConfig)config;
        this.classificationLabel = classificationLabel;
        this.topClasses = topClasses == null ? Collections.emptyList() : Collections.unmodifiableList(topClasses);
        this.topNumClassesField = classificationConfig.getTopClassesResultsField();
        this.resultsField = classificationConfig.getResultsField();
    }

    public ClassificationInferenceResults(StreamInput in) throws IOException {
        super(in);
        this.classificationLabel = in.readOptionalString();
        this.topClasses = Collections.unmodifiableList(in.readList(TopClassEntry::new));
        this.topNumClassesField = in.readString();
        this.resultsField = in.readString();
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
        out.writeCollection(topClasses);
        out.writeString(topNumClassesField);
        out.writeString(resultsField);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        ClassificationInferenceResults that = (ClassificationInferenceResults) object;
        return Objects.equals(value(), that.value()) &&
            Objects.equals(classificationLabel, that.classificationLabel) &&
            Objects.equals(resultsField, that.resultsField) &&
            Objects.equals(topNumClassesField, that.topNumClassesField) &&
            Objects.equals(topClasses, that.topClasses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value(), classificationLabel, topClasses, resultsField, topNumClassesField);
    }

    @Override
    public String valueAsString() {
        return classificationLabel == null ? super.valueAsString() : classificationLabel;
    }

    @Override
    public void writeResult(IngestDocument document, String parentResultField) {
        ExceptionsHelper.requireNonNull(document, "document");
        ExceptionsHelper.requireNonNull(parentResultField, "parentResultField");
        document.setFieldValue(parentResultField + "." + this.resultsField, valueAsString());
        if (topClasses.size() > 0) {
            document.setFieldValue(parentResultField + "." + topNumClassesField,
                topClasses.stream().map(TopClassEntry::asValueMap).collect(Collectors.toList()));
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }


    public static class TopClassEntry implements Writeable {

        public final ParseField CLASSIFICATION = new ParseField("classification");
        public final ParseField PROBABILITY = new ParseField("probability");

        private final String classification;
        private final double probability;

        public TopClassEntry(String classification, Double probability) {
            this.classification = ExceptionsHelper.requireNonNull(classification, CLASSIFICATION);
            this.probability = ExceptionsHelper.requireNonNull(probability, PROBABILITY);
        }

        public TopClassEntry(StreamInput in) throws IOException {
            this.classification = in.readString();
            this.probability = in.readDouble();
        }

        public String getClassification() {
            return classification;
        }

        public double getProbability() {
            return probability;
        }

        public Map<String, Object> asValueMap() {
            Map<String, Object> map = new HashMap<>(2);
            map.put(CLASSIFICATION.getPreferredName(), classification);
            map.put(PROBABILITY.getPreferredName(), probability);
            return map;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(classification);
            out.writeDouble(probability);
        }

        @Override
        public boolean equals(Object object) {
            if (object == this) { return true; }
            if (object == null || getClass() != object.getClass()) { return false; }
            TopClassEntry that = (TopClassEntry) object;
            return Objects.equals(classification, that.classification) &&
                Objects.equals(probability, that.probability);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classification, probability);
        }
    }
}
