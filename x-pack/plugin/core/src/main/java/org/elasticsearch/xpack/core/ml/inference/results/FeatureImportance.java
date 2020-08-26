/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.Version;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class FeatureImportance implements Writeable, ToXContentObject {

    private final List<ClassImportance> classImportance;
    private final double importance;
    private final String featureName;
    static final String IMPORTANCE = "importance";
    static final String FEATURE_NAME = "feature_name";
    static final String CLASSES = "classes";

    public static FeatureImportance forRegression(String featureName, double importance) {
        return new FeatureImportance(featureName, importance, null);
    }

    public static FeatureImportance forClassification(String featureName, List<ClassImportance> classImportance) {
        return new FeatureImportance(featureName,
            classImportance.stream().mapToDouble(ClassImportance::getImportance).map(Math::abs).sum(),
            classImportance);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FeatureImportance, Void> PARSER =
        new ConstructingObjectParser<>("feature_importance",
            a -> new FeatureImportance((String) a[0], (Double) a[1], (List<ClassImportance>) a[2])
        );

    static {
        PARSER.declareString(constructorArg(), new ParseField(FeatureImportance.FEATURE_NAME));
        PARSER.declareDouble(constructorArg(), new ParseField(FeatureImportance.IMPORTANCE));
        PARSER.declareObjectArray(optionalConstructorArg(),
            (p, c) -> ClassImportance.fromXContent(p),
            new ParseField(FeatureImportance.CLASSES));
    }

    public static FeatureImportance fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    FeatureImportance(String featureName, double importance, List<ClassImportance> classImportance) {
        this.featureName = Objects.requireNonNull(featureName);
        this.importance = importance;
        this.classImportance = classImportance == null ? null : Collections.unmodifiableList(classImportance);
    }

    public FeatureImportance(StreamInput in) throws IOException {
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

    public List<ClassImportance> getClassImportance() {
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
            if (out.getVersion().before(Version.V_7_10_0)) {
                out.writeMap(ClassImportance.toMap(this.classImportance), StreamOutput::writeString, StreamOutput::writeDouble);
            } else {
                out.writeList(this.classImportance);
            }
        }
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(FEATURE_NAME, featureName);
        map.put(IMPORTANCE, importance);
        if (classImportance != null) {
            map.put(CLASSES, classImportance.stream().map(ClassImportance::toMap).collect(Collectors.toList()));
        }
        return map;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FEATURE_NAME, featureName);
        builder.field(IMPORTANCE, importance);
        if (classImportance != null && classImportance.isEmpty() == false) {
            builder.field(CLASSES, classImportance);
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

    public static class ClassImportance implements Writeable, ToXContentObject {

        static final String CLASS_NAME = "class_name";

        private static final ConstructingObjectParser<ClassImportance, Void> PARSER =
            new ConstructingObjectParser<>("feature_importance_class_importance",
                a -> new ClassImportance((String) a[0], (Double) a[1])
            );

        static {
            PARSER.declareString(constructorArg(), new ParseField(CLASS_NAME));
            PARSER.declareDouble(constructorArg(), new ParseField(FeatureImportance.IMPORTANCE));
        }

        private static ClassImportance fromMapEntry(Map.Entry<String, Double> entry) {
            return new ClassImportance(entry.getKey(), entry.getValue());
        }

        private static List<ClassImportance> fromMap(Map<String, Double> classImportanceMap) {
            return classImportanceMap.entrySet().stream().map(ClassImportance::fromMapEntry).collect(Collectors.toList());
        }

        private static Map<String, Double> toMap(List<ClassImportance> importances) {
            return importances.stream().collect(Collectors.toMap(i -> i.className, i -> i.importance));
        }

        public static ClassImportance fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final String className;
        private final double importance;

        public ClassImportance(String className, double importance) {
            this.className = className;
            this.importance = importance;
        }

        public ClassImportance(StreamInput in) throws IOException {
            this.className = in.readString();
            this.importance = in.readDouble();
        }

        public String getClassName() {
            return className;
        }

        public double getImportance() {
            return importance;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put(CLASS_NAME, className);
            map.put(IMPORTANCE, importance);
            return map;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(className);
            out.writeDouble(importance);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASS_NAME, className);
            builder.field(IMPORTANCE, importance);
            builder.endObject();
            return builder;
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
