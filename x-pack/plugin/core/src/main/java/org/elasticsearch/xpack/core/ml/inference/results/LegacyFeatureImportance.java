/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class captures serialization of feature importance for
 * classification and regression prior to version 7.10.
 */
public class LegacyFeatureImportance implements Writeable {

    public static LegacyFeatureImportance fromClassification(ClassificationFeatureImportance classificationFeatureImportance) {
        return new LegacyFeatureImportance(
            classificationFeatureImportance.getFeatureName(),
            classificationFeatureImportance.getTotalImportance(),
            classificationFeatureImportance.getClassImportance().stream().map(classImportance -> new ClassImportance(
                classImportance.getClassName(), classImportance.getImportance())).collect(Collectors.toList())
        );
    }

    public static LegacyFeatureImportance fromRegression(RegressionFeatureImportance regressionFeatureImportance) {
        return new LegacyFeatureImportance(
            regressionFeatureImportance.getFeatureName(),
            regressionFeatureImportance.getImportance(),
            null
        );
    }

    private final List<ClassImportance> classImportance;
    private final double importance;
    private final String featureName;

    LegacyFeatureImportance(String featureName, double importance, List<ClassImportance> classImportance) {
        this.featureName = Objects.requireNonNull(featureName);
        this.importance = importance;
        this.classImportance = classImportance == null ? null : Collections.unmodifiableList(classImportance);
    }

    public LegacyFeatureImportance(StreamInput in) throws IOException {
        this.featureName = in.readString();
        this.importance = in.readDouble();
        if (in.readBoolean()) {
            if (in.getVersion().before(Version.V_7_10_0)) {
                Map<String, Double> classImportance = in.readMap(StreamInput::readString, StreamInput::readDouble);
                this.classImportance = ClassImportance.fromMap(classImportance);
            } else {
                this.classImportance = in.readList(ClassImportance::new);
            }
        } else {
            this.classImportance = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(featureName);
        out.writeDouble(importance);
        out.writeBoolean(classImportance != null);
        if (classImportance != null) {
            if (out.getVersion().before(Version.V_7_10_0)) {
                out.writeMap(ClassImportance.toMap(classImportance), StreamOutput::writeString, StreamOutput::writeDouble);
            } else {
                out.writeList(classImportance);
            }
        }
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        LegacyFeatureImportance that = (LegacyFeatureImportance) object;
        return Objects.equals(featureName, that.featureName)
            && Objects.equals(importance, that.importance)
            && Objects.equals(classImportance, that.classImportance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureName, importance, classImportance);
    }

    public RegressionFeatureImportance forRegression() {
        assert classImportance == null;
        return new RegressionFeatureImportance(featureName, importance);
    }

    public ClassificationFeatureImportance forClassification() {
        assert classImportance != null;
        return new ClassificationFeatureImportance(featureName, classImportance.stream().map(
            aClassImportance -> new ClassificationFeatureImportance.ClassImportance(
                aClassImportance.className, aClassImportance.importance)).collect(Collectors.toList()));
    }

    public static class ClassImportance implements Writeable {

        private static ClassImportance fromMapEntry(Map.Entry<String, Double> entry) {
            return new ClassImportance(entry.getKey(), entry.getValue());
        }

        private static List<ClassImportance> fromMap(Map<String, Double> classImportanceMap) {
            return classImportanceMap.entrySet().stream().map(ClassImportance::fromMapEntry).collect(Collectors.toList());
        }

        private static Map<String, Double> toMap(List<ClassImportance> importances) {
            return importances.stream().collect(Collectors.toMap(i -> i.className.toString(), i -> i.importance));
        }

        private final Object className;
        private final double importance;

        public ClassImportance(Object className, double importance) {
            this.className = className;
            this.importance = importance;
        }

        public ClassImportance(StreamInput in) throws IOException {
            this.className = in.readGenericValue();
            this.importance = in.readDouble();
        }

        double getImportance() {
            return importance;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(className);
            out.writeDouble(importance);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClassImportance that = (ClassImportance) o;
            return Double.compare(that.importance, importance) == 0 &&
                Objects.equals(className, that.className);
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, importance);
        }
    }
}
