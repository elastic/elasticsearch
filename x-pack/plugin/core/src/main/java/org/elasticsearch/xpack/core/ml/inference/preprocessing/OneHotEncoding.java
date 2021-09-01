/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * PreProcessor for one hot encoding a set of categorical values for a given field.
 */
public class OneHotEncoding implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(OneHotEncoding.class);
    public static final ParseField NAME = new ParseField("one_hot_encoding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField HOT_MAP = new ParseField("hot_map");
    public static final ParseField CUSTOM = new ParseField("custom");

    private static final ConstructingObjectParser<OneHotEncoding, PreProcessorParseContext> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<OneHotEncoding, PreProcessorParseContext> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<OneHotEncoding, PreProcessorParseContext> createParser(boolean lenient) {
        ConstructingObjectParser<OneHotEncoding, PreProcessorParseContext> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            (a, c) -> new OneHotEncoding((String)a[0],
                (Map<String, String>)a[1],
                a[2] == null ? c.isCustomByDefault() : (Boolean)a[2]));
        parser.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        parser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HOT_MAP);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), CUSTOM);
        return parser;
    }

    public static OneHotEncoding fromXContentStrict(XContentParser parser, PreProcessorParseContext context) {
        return STRICT_PARSER.apply(parser, context == null ?  PreProcessorParseContext.DEFAULT : context);
    }

    public static OneHotEncoding fromXContentLenient(XContentParser parser, PreProcessorParseContext context) {
        return LENIENT_PARSER.apply(parser, context == null ?  PreProcessorParseContext.DEFAULT : context);
    }

    private final String field;
    private final Map<String, String> hotMap;
    private final boolean custom;

    public OneHotEncoding(String field, Map<String, String> hotMap, Boolean custom) {
        this.field = ExceptionsHelper.requireNonNull(field, FIELD);
        this.hotMap = Collections.unmodifiableMap(new TreeMap<>(ExceptionsHelper.requireNonNull(hotMap, HOT_MAP)));
        this.custom = custom != null && custom;
    }

    public OneHotEncoding(StreamInput in) throws IOException {
        this.field = in.readString();
        this.hotMap = Collections.unmodifiableMap(new TreeMap<>(in.readMap(StreamInput::readString, StreamInput::readString)));
        this.custom = in.readBoolean();
    }

    /**
     * @return Field name on which to one hot encode
     */
    public String getField() {
        return field;
    }

    /**
     * @return Map of Value: ColumnName for the one hot encoding
     */
    public Map<String, String> getHotMap() {
        return hotMap;
    }

    @Override
    public Map<String, String> reverseLookup() {
        return hotMap.values().stream().collect(Collectors.toMap(Function.identity(), (value) -> field));
    }

    @Override
    public boolean isCustom() {
        return custom;
    }

    @Override
    public String getOutputFieldType(String outputField) {
        return NumberFieldMapper.NumberType.INTEGER.typeName();
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public List<String> inputFields() {
        return Collections.singletonList(field);
    }

    @Override
    public List<String> outputFields() {
        return new ArrayList<>(hotMap.values());
    }

    @Override
    public void process(Map<String, Object> fields) {
        Object value = fields.get(field);
        if (value == null) {
            return;
        }
        final String stringValue = value.toString();
        hotMap.forEach((val, col) -> {
            int encoding = stringValue.equals(val) ? 1 : 0;
            fields.put(col, encoding);
        });
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeMap(hotMap, StreamOutput::writeString, StreamOutput::writeString);
        out.writeBoolean(custom);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(HOT_MAP.getPreferredName(), hotMap);
        builder.field(CUSTOM.getPreferredName(), custom);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OneHotEncoding that = (OneHotEncoding) o;
        return Objects.equals(field, that.field)
            && Objects.equals(hotMap, that.hotMap)
            && Objects.equals(custom, that.custom);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, hotMap, custom);
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(field);
        // defSize:0 does not do much in this case as sizeOf(String) is a known quantity
        size += RamUsageEstimator.sizeOfMap(hotMap, 0);
        return size;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
