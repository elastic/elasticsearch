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
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.escf.EscfLongColumn;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.TimestampBounds;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.core.TimeValue.NSEC_PER_MSEC;

/**
 * FieldMapper for the data-stream's timestamp meta-field.
 * This field is added by {@code DataStreamsPlugin}.
 */
public class DataStreamTimestampFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_data_stream_timestamp";
    public static final String DEFAULT_PATH = "@timestamp";
    public static final String TIMESTAMP_VALUE_KEY = "@timestamp._value";

    public static final DataStreamTimestampFieldMapper ENABLED_INSTANCE = new DataStreamTimestampFieldMapper(true);
    static final DataStreamTimestampFieldMapper DISABLED_INSTANCE = new DataStreamTimestampFieldMapper(false);

    // For now the field shouldn't be usable in searches.
    // In the future it should act as an alias to the actual data stream timestamp field.
    public static final class TimestampFieldType extends MappedFieldType {

        static final TimestampFieldType INSTANCE = new TimestampFieldType();

        private TimestampFieldType() {
            super(NAME, IndexType.NONE, false, Map.of());
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
            .setMergeValidator((previous, current, ignored) -> (previous == current) || (previous == false && current));

        public Builder() {
            super(NAME);
        }

        public boolean isEnabled() {
            return enabled.getValue();
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { enabled };
        }

        @Override
        public String contentType() {
            return NAME;
        }

        /**
         * The {@code @timestamp} field of a data stream may only carry the attributes listed here. Which ones the user
         * set explicitly is read from the field's builder, which serializes only those that differ from their defaults.
         * A missing or non-date {@code @timestamp} is left for {@link #doValidate} to report.
         */
        @Override
        void validateMapping(RootObjectMapper.Builder root) {
            if (enabled.getValue() == false) {
                return;
            }
            for (Mapper.Builder child : root.getChildBuilders()) {
                if (DEFAULT_PATH.equals(child.leafName()) && child instanceof DateFieldMapper.Builder dateBuilder) {
                    validateTimestampSettings(dateBuilder);
                }
            }
        }

        private static void validateTimestampSettings(DateFieldMapper.Builder dateBuilder) {
            // A timestamp field can configure multi-fields, copy_to and synthetic_source_keep; all other
            // configuration parameters need specific values to survive validation.
            Set<String> disallowed = new TreeSet<>();
            for (Parameter<?> parameter : dateBuilder.getParameters()) {
                if (parameter.isConfigured() == false) {
                    continue;
                }
                switch (parameter.name) {
                    case "meta", "format", "locale", "ignore_malformed" -> {
                        // ignoring malformed values is disallowed (see the check in doValidate), however if
                        // `index.mapping.ignore_malformed` has been set to true then there is no way to disable ignore_malformed
                        // for the timestamp field mapper, other than not using 'index.mapping.ignore_malformed' at all. So by
                        // ignoring it here, we allow the index setting to be set to true and then turned off for the timestamp field.
                    }
                    case "null_value" -> {
                        // any configured null_value is non-null, which doValidate rejects with a message of its own
                    }
                    case "doc_values" -> {
                        // disabled doc values are rejected by doValidate with a message of its own
                        if (parameter instanceof FieldMapper.DocValuesParameter docValues && docValues.getValue().enabled()) {
                            disallowed.add(parameter.name);
                        }
                    }
                    case "index" -> {
                        if (Boolean.FALSE.equals(parameter.getValue())) {
                            throw new IllegalArgumentException(
                                "data stream timestamp field [@timestamp] indexing can't be explicitly disabled"
                            );
                        }
                    }
                    default -> disallowed.add(parameter.name);
                }
            }
            if (dateBuilder.multiFieldsBuilder.hasMultiFields()) {
                disallowed.add("fields");
            }
            if (dateBuilder.copyTo.copyToFields().isEmpty() == false) {
                disallowed.add("copy_to");
            }
            if (dateBuilder.sourceKeepMode.isPresent()) {
                disallowed.add(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM);
            }
            if (disallowed.isEmpty() == false) {
                throw new IllegalArgumentException("data stream timestamp field [@timestamp] has disallowed attributes: " + disallowed);
            }
        }

        @Override
        public MetadataFieldMapper build() {
            return enabled.getValue() ? ENABLED_INSTANCE : DISABLED_INSTANCE;
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(ignored -> new Builder());

    private final boolean enabled;

    private DataStreamTimestampFieldMapper(boolean enabled) {
        super(TimestampFieldType.INSTANCE);
        this.enabled = enabled;
    }

    @Override
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
                Strings.format(
                    "data stream timestamp field [%s] is of type [%s], but [%s,%s] is expected",
                    DEFAULT_PATH,
                    mapper.typeName(),
                    DateFieldMapper.CONTENT_TYPE,
                    DateFieldMapper.DATE_NANOS_CONTENT_TYPE
                )
            );
        }

        DateFieldMapper dateFieldMapper = (DateFieldMapper) mapper;

        IndexType indexType = dateFieldMapper.fieldType().indexType();
        if (indexType.hasPoints() == false && indexType.hasDocValuesSkipper() == false) {
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

        // Timestamp presence is always required; but bounds are only checked when shouldValidateTimestamp is true.
        long timestamp = extractTimestampValue(context.doc()); // this throws if the timestamp is missing

        var indexMode = context.indexSettings().getMode();
        if (indexMode.shouldValidateTimestamp()) {
            TimestampBounds bounds = context.indexSettings().getTimestampBounds();
            validateTimestamp(bounds, timestamp, context);
        }
    }

    private static void validateTimestamp(TimestampBounds bounds, long originValue, DocumentParserContext context) {
        boolean isDateNanos = context.mappingLookup()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .typeName()
            .equals(DateFieldMapper.DATE_NANOS_CONTENT_TYPE);
        validateTimestampValue(bounds, originValue, isDateNanos);
    }

    private static void validateTimestampValue(TimestampBounds bounds, long originValue, boolean isDateNanos) {
        long value = originValue;
        Resolution resolution;
        if (isDateNanos) {
            resolution = Resolution.NANOSECONDS;
            value /= NSEC_PER_MSEC;
        } else {
            resolution = Resolution.MILLISECONDS;
        }

        final long startTime = bounds.startTime();
        if (value < startTime) {
            throw new IllegalArgumentException(
                Strings.format(
                    "time series index @timestamp value [%s] must be larger than %s",
                    resolution.toInstant(originValue),
                    Instant.ofEpochMilli(startTime)
                )
            );
        }

        final long endTime = bounds.endTime();
        if (value >= endTime) {
            throw new IllegalArgumentException(
                Strings.format(
                    "time series index @timestamp value [%s] must be smaller than %s",
                    resolution.toInstant(originValue),
                    Instant.ofEpochMilli(endTime)
                )
            );
        }
    }

    @Override
    protected boolean doSupportsColumnarParse(IndexSettings indexSettings) {
        return true;
    }

    @Override
    public void postColumnarParse(BatchMappingContext context) throws IOException {
        if (enabled == false) {
            // not configured, so skip
            return;
        }

        // Presence is always required; bounds are only checked when the index mode demands it.
        // Unlike the row-based postParse (called per-document, failures isolated by the engine's
        // bulk loop), this method runs once for the entire batch. Any violation rejects the whole
        // batch. This is intentional: the columnar write path has no partial-commit mechanism.
        EscfLongColumn timestamps = context.timestamps(); // n.b. throws if not set
        if (timestamps.isDense() == false || timestamps.docCount() != context.docCount()) {
            throw new IllegalArgumentException("data stream timestamp field [" + DEFAULT_PATH + "] is missing");
        }

        final IndexSettings indexSettings = context.indexSettings();
        if (indexSettings.getMode().shouldValidateTimestamp()) {
            TimestampBounds bounds = indexSettings.getTimestampBounds();
            boolean isDateNanos = context.mappingLookup()
                .getMapper(DEFAULT_PATH)
                .typeName()
                .equals(DateFieldMapper.DATE_NANOS_CONTENT_TYPE);
            LongValuesCursor cursor = timestamps.longValuesCursor();
            for (int doc = 0; doc < context.docCount(); doc++) {
                long timestamp = cursor.nextLong();
                try {
                    validateTimestampValue(bounds, timestamp, isDateNanos);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(Strings.format("document [%d]: %s", doc, e.getMessage()), e);
                }
            }
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
