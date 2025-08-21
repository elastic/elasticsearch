/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.TimestampBounds;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Map;

import static org.elasticsearch.core.TimeValue.NSEC_PER_MSEC;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * FieldMapper for the data-stream's timestamp meta-field.
 * This field is added by {@code DataStreamsPlugin}.
 */
public class DataStreamTimestampFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_data_stream_timestamp";
    public static final String DEFAULT_PATH = "@timestamp";
    public static final String TIMESTAMP_VALUE_KEY = "@timestamp._value";

    public static final DataStreamTimestampFieldMapper ENABLED_INSTANCE = new DataStreamTimestampFieldMapper(true);
    private static final DataStreamTimestampFieldMapper DISABLED_INSTANCE = new DataStreamTimestampFieldMapper(false);

    // For now the field shouldn't be useable in searches.
    // In the future it should act as an alias to the actual data stream timestamp field.
    public static final class TimestampFieldType extends MappedFieldType {

        static final TimestampFieldType INSTANCE = new TimestampFieldType();

        private TimestampFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Map.of());
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support term queries");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support exists queries");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException();
        }
    }

    private static DataStreamTimestampFieldMapper toType(FieldMapper in) {
        return (DataStreamTimestampFieldMapper) in;
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Boolean> enabled = Parameter.boolParam("enabled", true, m -> toType(m).enabled, false)
            // this field mapper may be enabled but once enabled, may not be disabled
            .setMergeValidator((previous, current, conflicts) -> (previous == current) || (previous == false && current));

        public Builder() {
            super(NAME);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { enabled };
        }

        @Override
        public MetadataFieldMapper build() {
            return enabled.getValue() ? ENABLED_INSTANCE : DISABLED_INSTANCE;
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(c -> DISABLED_INSTANCE, c -> new Builder());

    private final boolean enabled;

    private DataStreamTimestampFieldMapper(boolean enabled) {
        super(TimestampFieldType.INSTANCE);
        this.enabled = enabled;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    public void doValidate(MappingLookup lookup) {
        if (enabled == false) {
            // not configured, so skip the validation
            return;
        }

        Mapper mapper = lookup.getMapper(DEFAULT_PATH);
        if (mapper == null) {
            throw new IllegalArgumentException("data stream timestamp field [" + DEFAULT_PATH + "] does not exist");
        }

        if (DateFieldMapper.CONTENT_TYPE.equals(mapper.typeName()) == false
            && DateFieldMapper.DATE_NANOS_CONTENT_TYPE.equals(mapper.typeName()) == false) {
            throw new IllegalArgumentException(
                "data stream timestamp field ["
                    + DEFAULT_PATH
                    + "] is of type ["
                    + mapper.typeName()
                    + "], but ["
                    + DateFieldMapper.CONTENT_TYPE
                    + ","
                    + DateFieldMapper.DATE_NANOS_CONTENT_TYPE
                    + "] is expected"
            );
        }

        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapper;
        if (dateFieldMapper.fieldType().isIndexed() == false && dateFieldMapper.fieldType().hasDocValuesSkipper() == false) {
            throw new IllegalArgumentException("data stream timestamp field [" + DEFAULT_PATH + "] is not indexed");
        }
        if (dateFieldMapper.fieldType().hasDocValues() == false) {
            throw new IllegalArgumentException("data stream timestamp field [" + DEFAULT_PATH + "] doesn't have doc values");
        }
        if (dateFieldMapper.getNullValue() != null) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + DEFAULT_PATH + "] has disallowed [null_value] attribute specified"
            );
        }
        if (dateFieldMapper.ignoreMalformed()) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + DEFAULT_PATH + "] has disallowed [ignore_malformed] attribute specified"
            );
        }

        // Catch all validation that validates whether disallowed mapping attributes have been specified
        // on the field this meta field refers to:
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            dateFieldMapper.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            Map<?, ?> configuredSettings = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
            configuredSettings = (Map<?, ?>) configuredSettings.values().iterator().next();

            // Only type, meta, format, and locale attributes are allowed:
            configuredSettings.remove("type");
            configuredSettings.remove("meta");
            configuredSettings.remove("format");
            configuredSettings.remove("locale");

            // ignoring malformed values is disallowed (see previous check),
            // however if `index.mapping.ignore_malformed` has been set to true then
            // there is no way to disable ignore_malformed for the timestamp field mapper,
            // other then not using 'index.mapping.ignore_malformed' at all.
            // So by ignoring the ignore_malformed here, we allow index.mapping.ignore_malformed
            // index setting to be set to true and then turned off for the timestamp field mapper.
            // (ignore_malformed will here always be false, otherwise previous check would have failed)
            Object value = configuredSettings.remove("ignore_malformed");
            assert value == null || Boolean.FALSE.equals(value);

            // All other configured attributes are not allowed:
            if (configuredSettings.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "data stream timestamp field [@timestamp] has disallowed attributes: " + configuredSettings.keySet()
                );
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void storeTimestampValueForReuse(LuceneDocument document, long timestamp) {
        var existingField = document.getByKey(DataStreamTimestampFieldMapper.TIMESTAMP_VALUE_KEY);
        if (existingField != null) {
            throw new IllegalArgumentException("data stream timestamp field [" + DEFAULT_PATH + "] encountered multiple values");
        }

        document.onlyAddKey(
            DataStreamTimestampFieldMapper.TIMESTAMP_VALUE_KEY,
            new LongField(DataStreamTimestampFieldMapper.TIMESTAMP_VALUE_KEY, timestamp, Field.Store.NO)
        );
    }

    public static long extractTimestampValue(LuceneDocument document) {
        IndexableField timestampValueField = document.getByKey(TIMESTAMP_VALUE_KEY);
        if (timestampValueField == null) {
            throw new IllegalArgumentException("data stream timestamp field [" + DEFAULT_PATH + "] is missing");
        }

        return timestampValueField.numericValue().longValue();
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        if (enabled == false) {
            // not configured, so skip the validation
            return;
        }

        long timestamp = extractTimestampValue(context.doc());

        var indexMode = context.indexSettings().getMode();
        if (indexMode.shouldValidateTimestamp()) {
            TimestampBounds bounds = context.indexSettings().getTimestampBounds();
            validateTimestamp(bounds, timestamp, context);
        }
    }

    private static void validateTimestamp(TimestampBounds bounds, long originValue, DocumentParserContext context) {
        long value = originValue;

        Resolution resolution;
        if (context.mappingLookup()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .typeName()
            .equals(DateFieldMapper.DATE_NANOS_CONTENT_TYPE)) {
            resolution = Resolution.NANOSECONDS;
            value /= NSEC_PER_MSEC;
        } else {
            resolution = Resolution.MILLISECONDS;
        }

        final long startTime = bounds.startTime();
        if (value < startTime) {
            throw new IllegalArgumentException(
                "time series index @timestamp value ["
                    + resolution.toInstant(originValue)
                    + "] must be larger than "
                    + Instant.ofEpochMilli(startTime)
            );
        }

        final long endTime = bounds.endTime();
        if (value >= endTime) {
            throw new IllegalArgumentException(
                "time series index @timestamp value ["
                    + resolution.toInstant(originValue)
                    + "] must be smaller than "
                    + Instant.ofEpochMilli(endTime)
            );
        }
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
