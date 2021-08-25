/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * FieldMapper for the data-stream's timestamp meta-field.
 * This field is added by {@code DataStreamsPlugin}.
 */
public class DataStreamTimestampFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_data_stream_timestamp";
    private static final String DEFAULT_PATH = "@timestamp";

    // For now the field shouldn't be useable in searches.
    // In the future it should act as an alias to the actual data stream timestamp field.
    public static final class TimestampFieldType extends MappedFieldType {

        public TimestampFieldType() {
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
        protected List<Parameter<?>> getParameters() {
            return List.of(enabled);
        }

        @Override
        public MetadataFieldMapper build() {
            return new DataStreamTimestampFieldMapper(new TimestampFieldType(), enabled.getValue());
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(
        c -> new DataStreamTimestampFieldMapper(new TimestampFieldType(), false),
        c -> new Builder()
    );

    private final String path = DEFAULT_PATH;
    private final boolean enabled;

    private DataStreamTimestampFieldMapper(MappedFieldType mappedFieldType, boolean enabled) {
        super(mappedFieldType);
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

        Mapper mapper = lookup.getMapper(path);
        if (mapper == null) {
            throw new IllegalArgumentException("data stream timestamp field [" + path + "] does not exist");
        }

        if (DateFieldMapper.CONTENT_TYPE.equals(mapper.typeName()) == false
            && DateFieldMapper.DATE_NANOS_CONTENT_TYPE.equals(mapper.typeName()) == false) {
            throw new IllegalArgumentException(
                "data stream timestamp field ["
                    + path
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
        if (dateFieldMapper.fieldType().isSearchable() == false) {
            throw new IllegalArgumentException("data stream timestamp field [" + path + "] is not indexed");
        }
        if (dateFieldMapper.fieldType().hasDocValues() == false) {
            throw new IllegalArgumentException("data stream timestamp field [" + path + "] doesn't have doc values");
        }
        if (dateFieldMapper.getNullValue() != null) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + path + "] has disallowed [null_value] attribute specified"
            );
        }
        if (dateFieldMapper.getIgnoreMalformed()) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + path + "] has disallowed [ignore_malformed] attribute specified"
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

            // Only type, meta and format attributes are allowed:
            configuredSettings.remove("type");
            configuredSettings.remove("meta");
            configuredSettings.remove("format");

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

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        if (enabled == false) {
            // not configured, so skip the validation
            return;
        }

        IndexableField[] fields = context.rootDoc().getFields(path);
        if (fields.length == 0) {
            throw new IllegalArgumentException("data stream timestamp field [" + path + "] is missing");
        }

        long numberOfValues = Arrays.stream(fields)
            .filter(indexableField -> indexableField.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC)
            .count();
        if (numberOfValues > 1) {
            throw new IllegalArgumentException("data stream timestamp field [" + path + "] encountered multiple values");
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
