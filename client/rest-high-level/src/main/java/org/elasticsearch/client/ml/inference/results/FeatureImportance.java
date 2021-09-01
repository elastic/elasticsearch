/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference.results;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class FeatureImportance implements ToXContentObject {

    public static final String IMPORTANCE = "importance";
    public static final String FEATURE_NAME = "feature_name";
    public static final String CLASSES = "classes";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FeatureImportance, Void> PARSER =
        new ConstructingObjectParser<>("feature_importance", true,
            a -> new FeatureImportance((String) a[0], (Double) a[1], (List<ClassImportance>) a[2])
        );

    static {
        PARSER.declareString(constructorArg(), new ParseField(FeatureImportance.FEATURE_NAME));
        PARSER.declareDouble(optionalConstructorArg(), new ParseField(FeatureImportance.IMPORTANCE));
        PARSER.declareObjectArray(optionalConstructorArg(),
            (p, c) -> ClassImportance.fromXContent(p),
            new ParseField(FeatureImportance.CLASSES));
    }

    public static FeatureImportance fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final List<ClassImportance> classImportance;
    private final Double importance;
    private final String featureName;

    public FeatureImportance(String featureName, Double importance, List<ClassImportance> classImportance) {
        this.featureName = Objects.requireNonNull(featureName);
        this.importance = importance;
        this.classImportance = classImportance == null ? null : Collections.unmodifiableList(classImportance);
    }

    public List<ClassImportance> getClassImportance() {
        return classImportance;
    }

    public Double getImportance() {
        return importance;
    }

    public String getFeatureName() {
        return featureName;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FEATURE_NAME, featureName);
        if (importance != null) {
            builder.field(IMPORTANCE, importance);
        }
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

    public static class ClassImportance implements ToXContentObject {

        static final String CLASS_NAME = "class_name";

        private static final ConstructingObjectParser<ClassImportance, Void> PARSER =
            new ConstructingObjectParser<>("feature_importance_class_importance",
                true,
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
            PARSER.declareDouble(constructorArg(), new ParseField(FeatureImportance.IMPORTANCE));
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

        public Object getClassName() {
            return className;
        }

        public double getImportance() {
            return importance;
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
