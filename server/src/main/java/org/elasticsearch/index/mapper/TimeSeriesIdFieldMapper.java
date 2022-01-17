/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Supplier;

/**
 * Mapper for {@code _tsid} field included generated when the index is
 * {@link IndexMode#TIME_SERIES organized into time series}.
 */
public class TimeSeriesIdFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_tsid";
    public static final String CONTENT_TYPE = "_tsid";
    public static final TimeSeriesIdFieldType FIELD_TYPE = new TimeSeriesIdFieldType();
    public static final TimeSeriesIdFieldMapper INSTANCE = new TimeSeriesIdFieldMapper();

    /**
     * The maximum length of the tsid. The value itself comes from a range check in
     * Lucene's writer for utf-8 doc values.
     */
    private static final int LIMIT = ByteBlockPool.BYTE_BLOCK_SIZE - 2;
    /**
     * Maximum length of the name of dimension. We picked this so that we could
     * comfortable fit 16 dimensions inside {@link #LIMIT}.
     */
    private static final int DIMENSION_NAME_LIMIT = 512;
    /**
     * The maximum length of any single dimension. We picked this so that we could
     * comfortable fit 16 dimensions inside {@link #LIMIT}. This should be quite
     * comfortable given that dimensions are typically going to be less than a
     * hundred bytes each, but we're being paranoid here.
     */
    private static final int DIMENSION_VALUE_LIMIT = 1024;

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    public static class Builder extends MetadataFieldMapper.Builder {
        protected Builder() {
            super(NAME);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of();
        }

        @Override
        public TimeSeriesIdFieldMapper build() {
            return INSTANCE;
        }
    }

    public static final TypeParser PARSER = new FixedTypeParser(c -> c.getIndexSettings().getMode().buildTimeSeriesIdFieldMapper());

    public static final class TimeSeriesIdFieldType extends MappedFieldType {
        private TimeSeriesIdFieldType() {
            super(NAME, false, false, true, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this));
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return DocValueFormat.TIME_SERIES_ID;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            // TODO don't leak the TSID's binary format into the script
            return new SortedSetOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new DelegateDocValuesField(
                    new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(FieldData.toString(dv))),
                    n
                )
            );
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("[" + NAME + "] is not searchable");
        }
    }

    private TimeSeriesIdFieldMapper() {
        super(FIELD_TYPE);
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        assert fieldType().isIndexed() == false;

        // SortedMap is expected to be sorted by key (field name)
        SortedMap<String, BytesReference> dimensionFields = context.doc().getDimensionBytes();
        if (dimensionFields.isEmpty()) {
            throw new IllegalArgumentException("Dimension fields are missing.");
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(dimensionFields.size());
            for (Map.Entry<String, BytesReference> entry : dimensionFields.entrySet()) {
                String fieldName = entry.getKey();
                BytesRef fieldNameBytes = new BytesRef(fieldName);
                int len = fieldNameBytes.length;
                if (len > DIMENSION_NAME_LIMIT) {
                    throw new IllegalArgumentException(
                        "Dimension name must be less than [" + DIMENSION_NAME_LIMIT + "] bytes but [" + fieldName + "] was [" + len + "]."
                    );
                }
                // Write field name in utf-8 instead of writeString's utf-16-ish thing
                out.writeBytesRef(fieldNameBytes);
                entry.getValue().writeTo(out);
            }

            BytesReference timeSeriesId = out.bytes();
            if (timeSeriesId.length() > LIMIT) {
                throw new IllegalArgumentException(NAME + " longer than [" + LIMIT + "] bytes [" + timeSeriesId.length() + "].");
            }
            assert timeSeriesId != null : "In time series mode _tsid cannot be null";
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), timeSeriesId.toBytesRef()));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Decode the {@code _tsid} into a human readable map.
     */
    public static Map<String, Object> decodeTsid(StreamInput in) {
        try {
            int size = in.readVInt();
            Map<String, Object> result = new LinkedHashMap<String, Object>(size);

            for (int i = 0; i < size; i++) {
                String name = in.readString();

                int type = in.read();
                switch (type) {
                    case (byte) 's' -> // parse a string
                        result.put(name, in.readBytesRef().utf8ToString());
                    case (byte) 'l' -> // parse a long
                        result.put(name, in.readLong());
                    case (byte) 'u' -> { // parse an unsigned_long
                        Object ul = DocValueFormat.UnsignedLongShiftedDocValueFormat.INSTANCE.format(in.readLong());
                        result.put(name, ul);
                    }
                    default -> throw new IllegalArgumentException("Cannot parse [" + name + "]: Unknown type [" + type + "]");
                }
            }
            return result;
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Error formatting " + NAME + ": " + e.getMessage(), e);
        }
    }

    public static BytesReference encodeTsidValue(String value) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.write((byte) 's');
            /*
             * Write in utf8 instead of StreamOutput#writeString which is utf-16-ish
             * so its easier for folks to reason about the space taken up. Mostly
             * it'll be smaller too.
             */
            BytesRef bytes = new BytesRef(value);
            if (bytes.length > DIMENSION_VALUE_LIMIT) {
                throw new IllegalArgumentException(
                    "Dimension fields must be less than [" + DIMENSION_VALUE_LIMIT + "] bytes but was [" + bytes.length + "]."
                );
            }
            out.writeBytesRef(bytes);
            return out.bytes();
        } catch (IOException e) {
            throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
        }
    }

    public static BytesReference encodeTsidValue(long value) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.write((byte) 'l');
            out.writeLong(value);
            return out.bytes();
        } catch (IOException e) {
            throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
        }
    }

    public static BytesReference encodeTsidUnsignedLongValue(long value) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.write((byte) 'u');
            out.writeLong(value);
            return out.bytes();
        } catch (IOException e) {
            throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
        }
    }
}
