/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

import org.elasticsearch.core.Nullable;
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
        return builder.map(asMap());
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

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(FEATURE_NAME.getPreferredName(), featureName);
        if (importance != null) {
            map.put(IMPORTANCE.getPreferredName(), importance.asMap());
        }
        if (classImportances.isEmpty() == false) {
            map.put(CLASSES.getPreferredName(), classImportances.stream().map(ClassImportance::asMap).collect(Collectors.toList()));
        }
        return map;
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
            return builder.map(asMap());
        }

        private Map<String, Object> asMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put(MEAN_MAGNITUDE.getPreferredName(), meanMagnitude);
            map.put(MIN.getPreferredName(), min);
            map.put(MAX.getPreferredName(), max);
            return map;
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
                a -> new ClassImportance(a[0], (Importance)a[1]));
            parser.declareField(ConstructingObjectParser.constructorArg(), (p, c) -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return p.text();
                } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    return p.numberValue();
                } else if (p.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                    return p.booleanValue();
                }
                throw new XContentParseException("Unsupported token [" + p.currentToken() + "]");
            }, CLASS_NAME, ObjectParser.ValueType.VALUE);
            parser.declareObject(ConstructingObjectParser.constructorArg(),
                ignoreUnknownFields ? Importance.LENIENT_PARSER : Importance.STRICT_PARSER,
                IMPORTANCE);
            return parser;
        }

        public static ClassImportance fromXContent(XContentParser parser, boolean lenient) throws IOException {
            return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
        }

        public final Object className;
        public final Importance importance;

        public ClassImportance(StreamInput in) throws IOException {
            this.className = in.readGenericValue();
            this.importance = new Importance(in);
        }

        ClassImportance(Object className, Importance importance) {
            this.className = className;
            this.importance = importance;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(className);
            importance.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.map(asMap());
        }

        private Map<String, Object> asMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put(CLASS_NAME.getPreferredName(), className);
            map.put(IMPORTANCE.getPreferredName(), importance.asMap());
            return map;
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
