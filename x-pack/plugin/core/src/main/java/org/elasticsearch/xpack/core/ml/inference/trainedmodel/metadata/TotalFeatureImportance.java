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
    public static final ParseField FEATURE_NAME = new ParseField("feature_name");
    public static final ParseField IMPORTANCE = new ParseField("importance");
    public static final ParseField CLASSES = new ParseField("classes");
    public static final ParseField MEAN_MAGNITUDE = new ParseField("mean_magnitude");
    public static final ParseField MIN = new ParseField("min");
    public static final ParseField MAX = new ParseField("max");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<TotalFeatureImportance, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<TotalFeatureImportance, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<TotalFeatureImportance, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TotalFeatureImportance, Void> parser = new ConstructingObjectParser<>(NAME,
            ignoreUnknownFields,
            a -> new TotalFeatureImportance((String)a[0], (Importance)a[1], (List<ClassImportance>)a[2]));
        parser.declareString(ConstructingObjectParser.constructorArg(), FEATURE_NAME);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(),
            ignoreUnknownFields ? Importance.LENIENT_PARSER : Importance.STRICT_PARSER,
            IMPORTANCE);
        parser.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(),
            ignoreUnknownFields ? ClassImportance.LENIENT_PARSER : ClassImportance.STRICT_PARSER,
            CLASSES);
        return parser;
    }

    public static TotalFeatureImportance fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    public final String featureName;
    public final Importance importance;
    public final List<ClassImportance> classImportances;

    public TotalFeatureImportance(StreamInput in) throws IOException {
        this.featureName = in.readString();
        this.importance = in.readOptionalWriteable(Importance::new);
        this.classImportances = in.readList(ClassImportance::new);
    }

    TotalFeatureImportance(String featureName, @Nullable Importance importance, @Nullable List<ClassImportance> classImportances) {
        this.featureName = featureName;
        this.importance = importance;
        this.classImportances = classImportances == null ? Collections.emptyList() : classImportances;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(featureName);
        out.writeOptionalWriteable(importance);
        out.writeList(classImportances);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FEATURE_NAME.getPreferredName(), featureName);
        if (importance != null) {
            builder.field(IMPORTANCE.getPreferredName(), importance);
        }
        if (classImportances.isEmpty() == false) {
            builder.field(CLASSES.getPreferredName(), classImportances);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TotalFeatureImportance that = (TotalFeatureImportance) o;
        return Objects.equals(that.importance, importance)
            && Objects.equals(featureName, that.featureName)
            && Objects.equals(classImportances, that.classImportances);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureName, importance, classImportances);
    }

    public static class Importance implements ToXContentObject, Writeable {
        private static final String NAME = "importance";

        // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
        public static final ConstructingObjectParser<Importance, Void> LENIENT_PARSER = createParser(true);
        public static final ConstructingObjectParser<Importance, Void> STRICT_PARSER = createParser(false);

        private static ConstructingObjectParser<Importance, Void> createParser(boolean ignoreUnknownFields) {
            ConstructingObjectParser<Importance, Void> parser = new ConstructingObjectParser<>(NAME,
                ignoreUnknownFields,
                a -> new Importance((double)a[0], (double)a[1], (double)a[2]));
            parser.declareDouble(ConstructingObjectParser.constructorArg(), MEAN_MAGNITUDE);
            parser.declareDouble(ConstructingObjectParser.constructorArg(), MIN);
            parser.declareDouble(ConstructingObjectParser.constructorArg(), MAX);
            return parser;
        }

        private final double meanMagnitude;
        private final double min;
        private final double max;

        public Importance(double meanMagnitude, double min, double max) {
            this.meanMagnitude = meanMagnitude;
            this.min = min;
            this.max = max;
        }

        public Importance(StreamInput in) throws IOException {
            this.meanMagnitude = in.readDouble();
            this.min = in.readDouble();
            this.max = in.readDouble();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Importance that = (Importance) o;
            return Double.compare(that.meanMagnitude, meanMagnitude) == 0 &&
                Double.compare(that.min, min) == 0 &&
                Double.compare(that.max, max) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(meanMagnitude, min, max);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(meanMagnitude);
            out.writeDouble(min);
            out.writeDouble(max);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MEAN_MAGNITUDE.getPreferredName(), meanMagnitude);
            builder.field(MIN.getPreferredName(), min);
            builder.field(MAX.getPreferredName(), max);
            builder.endObject();
            return builder;
        }
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
                a -> new ClassImportance((String)a[0], (Importance)a[1]));
            parser.declareString(ConstructingObjectParser.constructorArg(), CLASS_NAME);
            parser.declareObject(ConstructingObjectParser.constructorArg(),
                ignoreUnknownFields ? Importance.LENIENT_PARSER : Importance.STRICT_PARSER,
                IMPORTANCE);
            return parser;
        }

        public static ClassImportance fromXContent(XContentParser parser, boolean lenient) throws IOException {
            return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
        }

        public final String className;
        public final Importance importance;

        public ClassImportance(StreamInput in) throws IOException {
            this.className = in.readString();
            this.importance = new Importance(in);
        }

        ClassImportance(String className, Importance importance) {
            this.className = className;
            this.importance = importance;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(className);
            importance.writeTo(out);
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
            return Objects.equals(that.importance, importance) && Objects.equals(className, that.className);
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, importance);
        }

    }
}
