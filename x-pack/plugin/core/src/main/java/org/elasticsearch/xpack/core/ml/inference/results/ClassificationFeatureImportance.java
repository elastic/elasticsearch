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

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ClassificationFeatureImportance extends AbstractFeatureImportance {

    private final List<ClassImportance> classImportance;
    private final String featureName;

    static final String FEATURE_NAME = "feature_name";
    static final String CLASSES = "classes";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ClassificationFeatureImportance, Void> PARSER =
        new ConstructingObjectParser<>("classification_feature_importance",
            a -> new ClassificationFeatureImportance((String) a[0], (List<ClassImportance>) a[1])
        );

    static {
        PARSER.declareString(constructorArg(), new ParseField(ClassificationFeatureImportance.FEATURE_NAME));
        PARSER.declareObjectArray(optionalConstructorArg(),
            (p, c) -> ClassImportance.fromXContent(p),
            new ParseField(ClassificationFeatureImportance.CLASSES));
    }

    public static ClassificationFeatureImportance fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ClassificationFeatureImportance(String featureName, List<ClassImportance> classImportance) {
        this.featureName = Objects.requireNonNull(featureName);
        this.classImportance = classImportance == null ? Collections.emptyList() : Collections.unmodifiableList(classImportance);
    }

    public ClassificationFeatureImportance(StreamInput in) throws IOException {
        this.featureName = in.readString();
        this.classImportance = in.readList(ClassImportance::new);
    }

    public List<ClassImportance> getClassImportance() {
        return classImportance;
    }

    @Override
    public String getFeatureName() {
        return featureName;
    }

    public double getTotalImportance() {
        if (classImportance.size() == 2) {
            // Binary classification. We can return the first class importance here
            return Math.abs(classImportance.get(0).getImportance());
        }
        return classImportance.stream().mapToDouble(ClassImportance::getImportance).map(Math::abs).sum();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(featureName);
        out.writeList(classImportance);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(FEATURE_NAME, featureName);
        if (classImportance.isEmpty() == false) {
            map.put(CLASSES, classImportance.stream().map(ClassImportance::toMap).collect(Collectors.toList()));
        }
        return map;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        ClassificationFeatureImportance that = (ClassificationFeatureImportance) object;
        return Objects.equals(featureName, that.featureName)
            && Objects.equals(classImportance, that.classImportance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureName, classImportance);
    }

    public static class ClassImportance implements Writeable, ToXContentObject {

        static final String CLASS_NAME = "class_name";
        static final String IMPORTANCE = "importance";

        private static final ConstructingObjectParser<ClassImportance, Void> PARSER =
            new ConstructingObjectParser<>("classification_feature_importance_class_importance",
                a -> new ClassImportance(a[0], (Double) a[1])
            );

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(), (p, c) -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return p.text();
                } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    return p.numberValue();
                } else if (p.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                    return p.booleanValue();
                }
                throw new XContentParseException("Unsupported token [" + p.currentToken() + "]");
            }, new ParseField(CLASS_NAME), ObjectParser.ValueType.VALUE);
            PARSER.declareDouble(constructorArg(), new ParseField(IMPORTANCE));
        }

        public static ClassImportance fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
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

        public Object getClassName() {
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
            out.writeGenericValue(className);
            out.writeDouble(importance);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.map(toMap());
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
