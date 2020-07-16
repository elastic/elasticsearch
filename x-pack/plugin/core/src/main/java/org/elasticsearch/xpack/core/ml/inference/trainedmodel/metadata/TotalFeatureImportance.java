/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

import org.elasticsearch.common.Nullable;
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
import java.util.List;
import java.util.Objects;

public class TotalFeatureImportance implements ToXContentObject, Writeable {

    private static final String NAME = "total_feature_importance";
    public static final ParseField FIELD_NAME = new ParseField("field_name");
    public static final ParseField IMPORTANCE = new ParseField("importance");
    public static final ParseField CLASS_IMPORTANCE = new ParseField("class_importance");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<TotalFeatureImportance, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<TotalFeatureImportance, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<TotalFeatureImportance, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TotalFeatureImportance, Void> parser = new ConstructingObjectParser<>(NAME,
            ignoreUnknownFields,
            a -> new TotalFeatureImportance((String)a[0], (Double)a[1], (List<ClassImportance>)a[2]));
        parser.declareString(ConstructingObjectParser.constructorArg(), FIELD_NAME);
        parser.declareDouble(ConstructingObjectParser.constructorArg(), IMPORTANCE);
        parser.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(),
            ignoreUnknownFields ? ClassImportance.LENIENT_PARSER : ClassImportance.STRICT_PARSER,
            CLASS_IMPORTANCE);
        return parser;
    }

    public static TotalFeatureImportance fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    public final String fieldName;
    public final double importance;
    public final List<ClassImportance> classImportances;

    public TotalFeatureImportance(StreamInput in) throws IOException {
        this.fieldName = in.readString();
        this.importance = in.readDouble();
        this.classImportances = in.readList(ClassImportance::new);
    }

    TotalFeatureImportance(String fieldName, double importance, @Nullable List<ClassImportance> classImportances) {
        this.fieldName = fieldName;
        this.importance = importance;
        this.classImportances = classImportances == null ? Collections.emptyList() : classImportances;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeDouble(importance);
        out.writeList(classImportances);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_NAME.getPreferredName(), fieldName);
        builder.field(IMPORTANCE.getPreferredName(), importance);
        if (classImportances.isEmpty() == false) {
            builder.field(CLASS_IMPORTANCE.getPreferredName(), classImportances);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TotalFeatureImportance that = (TotalFeatureImportance) o;
        return Double.compare(that.importance, importance) == 0
            && Objects.equals(fieldName, that.fieldName)
            && Objects.equals(classImportances, that.classImportances);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, importance, classImportances);
    }

    public static class ClassImportance implements ToXContentObject, Writeable {
        private static final String NAME = "total_class_importance";

        public static final ParseField CLASS_NAME = new ParseField("class_name");
        public static final ParseField IMPORTANCE = new ParseField("importance");

        // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
        public static final ConstructingObjectParser<ClassImportance, Void> LENIENT_PARSER = createParser(true);
        public static final ConstructingObjectParser<ClassImportance, Void> STRICT_PARSER = createParser(false);

        private static ConstructingObjectParser<ClassImportance, Void> createParser(boolean ignoreUnknownFields) {
            ConstructingObjectParser<ClassImportance, Void> parser = new ConstructingObjectParser<>(NAME,
                ignoreUnknownFields,
                a -> new ClassImportance((String)a[0], (Double)a[1]));
            parser.declareString(ConstructingObjectParser.constructorArg(), CLASS_NAME);
            parser.declareDouble(ConstructingObjectParser.constructorArg(), IMPORTANCE);
            return parser;
        }

        public static ClassImportance fromXContent(XContentParser parser, boolean lenient) throws IOException {
            return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
        }

        public final String className;
        public final double importance;

        public ClassImportance(StreamInput in) throws IOException {
            this.className = in.readString();
            this.importance = in.readDouble();
        }

        ClassImportance(String className, double importance) {
            this.className = className;
            this.importance = importance;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(className);
            out.writeDouble(importance);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASS_NAME.getPreferredName(), className);
            builder.field(IMPORTANCE.getPreferredName(), importance);
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
