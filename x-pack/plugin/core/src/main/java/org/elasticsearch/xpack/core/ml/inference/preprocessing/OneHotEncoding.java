/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * PreProcessor for one hot encoding a set of categorical values for a given field.
 */
public class OneHotEncoding implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(OneHotEncoding.class);
    public static final ParseField NAME = new ParseField("one_hot_encoding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField HOT_MAP = new ParseField("hot_map");

    public static final ConstructingObjectParser<OneHotEncoding, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<OneHotEncoding, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<OneHotEncoding, Void> createParser(boolean lenient) {
        ConstructingObjectParser<OneHotEncoding, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new OneHotEncoding((String)a[0], (Map<String, String>)a[1]));
        parser.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        parser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HOT_MAP);
        return parser;
    }

    public static OneHotEncoding fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static OneHotEncoding fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private final String field;
    private final Map<String, String> hotMap;

    public OneHotEncoding(String field, Map<String, String> hotMap) {
        this.field = ExceptionsHelper.requireNonNull(field, FIELD);
        this.hotMap = Collections.unmodifiableMap(ExceptionsHelper.requireNonNull(hotMap, HOT_MAP));
    }

    public OneHotEncoding(StreamInput in) throws IOException {
        this.field = in.readString();
        this.hotMap = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
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
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public void process(Map<String, Object> fields) {
        Object value = MapHelper.dig(field, fields);
        if (value == null) {
            return;
        }
        hotMap.forEach((val, col) -> {
            int encoding = value.toString().equals(val) ? 1 : 0;
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(HOT_MAP.getPreferredName(), hotMap);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OneHotEncoding that = (OneHotEncoding) o;
        return Objects.equals(field, that.field)
            && Objects.equals(hotMap, that.hotMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, hotMap);
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
