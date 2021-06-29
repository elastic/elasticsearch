/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

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

public class FeatureImportanceBaseline implements ToXContentObject, Writeable {

    private static final String NAME = "feature_importance_baseline";
    public static final ParseField BASELINE = new ParseField("baseline");
    public static final ParseField CLASSES = new ParseField("classes");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<FeatureImportanceBaseline, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<FeatureImportanceBaseline, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<FeatureImportanceBaseline, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<FeatureImportanceBaseline, Void> parser = new ConstructingObjectParser<>(NAME,
            ignoreUnknownFields,
            a -> new FeatureImportanceBaseline((Double)a[0], (List<ClassBaseline>)a[1]));
        parser.declareDouble(ConstructingObjectParser.optionalConstructorArg(), BASELINE);
        parser.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(),
            ignoreUnknownFields ? ClassBaseline.LENIENT_PARSER : ClassBaseline.STRICT_PARSER,
            CLASSES);
        return parser;
    }

    public static FeatureImportanceBaseline fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    public final Double baseline;
    public final List<ClassBaseline> classBaselines;

    public FeatureImportanceBaseline(StreamInput in) throws IOException {
        this.baseline = in.readOptionalDouble();
        this.classBaselines = in.readList(ClassBaseline::new);
    }

    public FeatureImportanceBaseline(Double baseline, List<ClassBaseline> classBaselines) {
        this.baseline = baseline;
        this.classBaselines = classBaselines == null ? Collections.emptyList() : classBaselines;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalDouble(baseline);
        out.writeList(classBaselines);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(asMap());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FeatureImportanceBaseline that = (FeatureImportanceBaseline) o;
        return Objects.equals(that.baseline, baseline)
            && Objects.equals(classBaselines, that.classBaselines);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (baseline != null) {
            map.put(BASELINE.getPreferredName(), baseline);
        }
        if (classBaselines.isEmpty() == false) {
            map.put(CLASSES.getPreferredName(), classBaselines.stream().map(ClassBaseline::asMap).collect(Collectors.toList()));
        }
        return map;
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseline, classBaselines);
    }

    public static class ClassBaseline implements ToXContentObject, Writeable {
        private static final String NAME = "feature_importance_class_baseline";

        public static final ParseField CLASS_NAME = new ParseField("class_name");

        public static final ConstructingObjectParser<ClassBaseline, Void> LENIENT_PARSER = createParser(true);
        public static final ConstructingObjectParser<ClassBaseline, Void> STRICT_PARSER = createParser(false);

        private static ConstructingObjectParser<ClassBaseline, Void> createParser(boolean ignoreUnknownFields) {
            ConstructingObjectParser<ClassBaseline, Void> parser = new ConstructingObjectParser<>(NAME,
                ignoreUnknownFields,
                a -> new ClassBaseline(a[0], (double)a[1]));
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
            parser.declareDouble(ConstructingObjectParser.constructorArg(), BASELINE);
            return parser;
        }

        public static ClassBaseline fromXContent(XContentParser parser, boolean lenient) throws IOException {
            return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
        }

        public final Object className;
        public final double baseline;

        public ClassBaseline(StreamInput in) throws IOException {
            this.className = in.readGenericValue();
            this.baseline = in.readDouble();
        }

        ClassBaseline(Object className, double baseline) {
            this.className = className;
            this.baseline = baseline;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(className);
            out.writeDouble(baseline);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.map(asMap());
        }

        private Map<String, Object> asMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put(CLASS_NAME.getPreferredName(), className);
            map.put(BASELINE.getPreferredName(), baseline);
            return map;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClassBaseline that = (ClassBaseline) o;
            return Objects.equals(that.className, className) && Objects.equals(baseline, that.baseline);
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, baseline);
        }

    }
}
