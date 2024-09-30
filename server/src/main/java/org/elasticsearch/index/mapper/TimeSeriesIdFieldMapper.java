/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.Murmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.net.InetAddress;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Mapper for {@code _tsid} field included generated when the index is
 * {@link IndexMode#TIME_SERIES organized into time series}.
 */
public class TimeSeriesIdFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_tsid";
    public static final String CONTENT_TYPE = "_tsid";
    public static final TimeSeriesIdFieldType FIELD_TYPE = new TimeSeriesIdFieldType();
    public static final TimeSeriesIdFieldMapper INSTANCE = new TimeSeriesIdFieldMapper();

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        protected Builder() {
            super(NAME);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return EMPTY_PARAMETERS;
        }

        @Override
        public TimeSeriesIdFieldMapper build() {
            return INSTANCE;
        }
    }

    public static final TypeParser PARSER = new FixedTypeParser(c -> c.getIndexSettings().getMode().timeSeriesIdFieldMapper());

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
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this, FielddataOperation.SEARCH));
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return DocValueFormat.TIME_SERIES_ID;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            // TODO don't leak the TSID's binary format into the script
            return new SortedOrdinalsIndexFieldData.Builder(
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

        final TimeSeriesIdBuilder timeSeriesIdBuilder = (TimeSeriesIdBuilder) context.getDimensions();
        final BytesRef timeSeriesId = getIndexVersionCreated(context).before(IndexVersions.TIME_SERIES_ID_HASHING)
            ? timeSeriesIdBuilder.buildLegacyTsid().toBytesRef()
            : timeSeriesIdBuilder.buildTsidHash().toBytesRef();
        context.doc().add(new SortedDocValuesField(fieldType().name(), timeSeriesId));
        TsidExtractingIdFieldMapper.createField(
            context,
            getIndexVersionCreated(context).before(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID)
                ? timeSeriesIdBuilder.routingBuilder
                : null,
            timeSeriesId
        );
    }

    private IndexVersion getIndexVersionCreated(final DocumentParserContext context) {
        return context.indexSettings().getIndexVersionCreated();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Decode the {@code _tsid} into a human readable map.
     */
    public static Object encodeTsid(StreamInput in) {
        try {
            return base64Encode(in.readSlicedBytesReference().toBytesRef());
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read tsid");
        }
    }

    public static class TimeSeriesIdBuilder implements DocumentDimensions {

        private static final int SEED = 0;

        public static final int MAX_DIMENSIONS = 512;

        private final Murmur3Hasher tsidHasher = new Murmur3Hasher(0);

        /**
         * A map of the serialized values of dimension fields that will be used
         * for generating the _tsid field. The map will be used by {@link TimeSeriesIdFieldMapper}
         * to build the _tsid field for the document.
         */
        private final SortedMap<BytesRef, List<BytesReference>> dimensions = new TreeMap<>();
        /**
         * Builds the routing. Used for building {@code _id}. If null then skipped.
         */
        @Nullable
        private final IndexRouting.ExtractFromSource.Builder routingBuilder;

        public TimeSeriesIdBuilder(@Nullable IndexRouting.ExtractFromSource.Builder routingBuilder) {
            this.routingBuilder = routingBuilder;
        }

        public BytesReference buildLegacyTsid() throws IOException {
            if (dimensions.isEmpty()) {
                throw new IllegalArgumentException("Dimension fields are missing.");
            }

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeVInt(dimensions.size());
                for (Map.Entry<BytesRef, List<BytesReference>> entry : dimensions.entrySet()) {
                    out.writeBytesRef(entry.getKey());
                    List<BytesReference> value = entry.getValue();
                    if (value.size() > 1) {
                        // multi-value dimensions are only supported for newer indices that use buildTsidHash
                        throw new IllegalArgumentException(
                            "Dimension field [" + entry.getKey().utf8ToString() + "] cannot be a multi-valued field."
                        );
                    }
                    assert value.isEmpty() == false : "dimension value is empty";
                    value.get(0).writeTo(out);
                }
                return out.bytes();
            }
        }

        private static final int MAX_HASH_LEN_BYTES = 2;

        static {
            assert MAX_HASH_LEN_BYTES == StreamOutput.putVInt(new byte[2], tsidHashLen(MAX_DIMENSIONS), 0);
        }

        /**
         * Here we build the hash of the tsid using a similarity function so that we have a result
         * with the following pattern:
         *
         * hash128(catenate(dimension field names)) +
         * foreach(dimension field value, limit = MAX_DIMENSIONS) { hash32(dimension field value) } +
         * hash128(catenate(dimension field values))
         *
         * The idea is to be able to place 'similar' time series close to each other. Two time series
         * are considered 'similar' if they share the same dimensions (names and values).
         */
        public BytesReference buildTsidHash() {
            // NOTE: hash all dimension field names
            int numberOfDimensions = Math.min(MAX_DIMENSIONS, dimensions.size());
            int len = tsidHashLen(numberOfDimensions);
            // either one or two bytes are occupied by the vint since we're bounded by #MAX_DIMENSIONS
            byte[] tsidHash = new byte[MAX_HASH_LEN_BYTES + len];
            int tsidHashIndex = StreamOutput.putVInt(tsidHash, len, 0);

            tsidHasher.reset();
            for (final BytesRef name : dimensions.keySet()) {
                tsidHasher.update(name.bytes);
            }
            tsidHashIndex = writeHash128(tsidHasher.digestHash(), tsidHash, tsidHashIndex);

            // NOTE: concatenate all dimension value hashes up to a certain number of dimensions
            int tsidHashStartIndex = tsidHashIndex;
            for (final List<BytesReference> values : dimensions.values()) {
                if ((tsidHashIndex - tsidHashStartIndex) >= 4 * numberOfDimensions) {
                    break;
                }
                assert values.isEmpty() == false : "dimension values are empty";
                final BytesRef dimensionValueBytesRef = values.get(0).toBytesRef();
                ByteUtils.writeIntLE(
                    StringHelper.murmurhash3_x86_32(
                        dimensionValueBytesRef.bytes,
                        dimensionValueBytesRef.offset,
                        dimensionValueBytesRef.length,
                        SEED
                    ),
                    tsidHash,
                    tsidHashIndex
                );
                tsidHashIndex += 4;
            }

            // NOTE: hash all dimension field allValues
            tsidHasher.reset();
            for (final List<BytesReference> values : dimensions.values()) {
                for (BytesReference v : values) {
                    tsidHasher.update(v.toBytesRef().bytes);
                }
            }
            tsidHashIndex = writeHash128(tsidHasher.digestHash(), tsidHash, tsidHashIndex);

            return new BytesArray(tsidHash, 0, tsidHashIndex);
        }

        private static int tsidHashLen(int numberOfDimensions) {
            return 16 + 16 + 4 * numberOfDimensions;
        }

        private int writeHash128(final MurmurHash3.Hash128 hash128, byte[] buffer, int tsidHashIndex) {
            ByteUtils.writeLongLE(hash128.h1, buffer, tsidHashIndex);
            tsidHashIndex += 8;
            ByteUtils.writeLongLE(hash128.h2, buffer, tsidHashIndex);
            tsidHashIndex += 8;
            return tsidHashIndex;
        }

        @Override
        public DocumentDimensions addString(String fieldName, BytesRef utf8Value) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.write((byte) 's');
                /*
                 * Write in utf8 instead of StreamOutput#writeString which is utf-16-ish
                 * so it's easier for folks to reason about the space taken up. Mostly
                 * it'll be smaller too.
                 */
                out.writeBytesRef(utf8Value);
                add(fieldName, out.bytes());

                if (routingBuilder != null) {
                    routingBuilder.addMatching(fieldName, utf8Value);
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
            }
            return this;
        }

        @Override
        public DocumentDimensions addIp(String fieldName, InetAddress value) {
            return addString(fieldName, NetworkAddress.format(value));
        }

        @Override
        public DocumentDimensions addLong(String fieldName, long value) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.write((byte) 'l');
                out.writeLong(value);
                add(fieldName, out.bytes());
            } catch (IOException e) {
                throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
            }
            return this;
        }

        @Override
        public DocumentDimensions addUnsignedLong(String fieldName, long value) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                Object ul = DocValueFormat.UNSIGNED_LONG_SHIFTED.format(value);
                if (ul instanceof Long l) {
                    out.write((byte) 'l');
                    out.writeLong(l);
                } else {
                    out.write((byte) 'u');
                    out.writeLong(value);
                }
                add(fieldName, out.bytes());
                return this;
            } catch (IOException e) {
                throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
            }
        }

        @Override
        public DocumentDimensions addBoolean(String fieldName, boolean value) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.write((byte) 'b');
                out.write(value ? 't' : 'f');
                add(fieldName, out.bytes());
            } catch (IOException e) {
                throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
            }
            return this;
        }

        @Override
        public DocumentDimensions validate(final IndexSettings settings) {
            if (settings.getIndexVersionCreated().before(IndexVersions.TIME_SERIES_ID_HASHING)
                && dimensions.size() > settings.getValue(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING)) {
                throw new MapperException(
                    "Too many dimension fields ["
                        + dimensions.size()
                        + "], max ["
                        + settings.getValue(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING)
                        + "] dimension fields allowed"
                );
            }
            return this;
        }

        private void add(String fieldName, BytesReference encoded) throws IOException {
            BytesRef name = new BytesRef(fieldName);
            List<BytesReference> values = dimensions.get(name);
            if (values == null) {
                // optimize for the common case where dimensions are not multi-valued
                dimensions.put(name, List.of(encoded));
            } else {
                if (values.size() == 1) {
                    // converts the immutable list that's optimized for the common case of having only one value to a mutable list
                    BytesReference previousValue = values.get(0);
                    values = new ArrayList<>(4);
                    values.add(previousValue);
                    dimensions.put(name, values);
                }
                values.add(encoded);
            }
        }
    }

    public static Object encodeTsid(final BytesRef bytesRef) {
        return base64Encode(bytesRef);
    }

    private static String base64Encode(final BytesRef bytesRef) {
        byte[] bytes = new byte[bytesRef.length];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, bytes, 0, bytesRef.length);
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(bytes);
    }

    public static Map<String, Object> decodeTsidAsMap(BytesRef bytesRef) {
        try (StreamInput input = new BytesArray(bytesRef).streamInput()) {
            return decodeTsidAsMap(input);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Dimension field cannot be deserialized.", ex);
        }
    }

    public static Map<String, Object> decodeTsidAsMap(StreamInput in) {
        try {
            int size = in.readVInt();
            Map<String, Object> result = new LinkedHashMap<>(size);

            for (int i = 0; i < size; i++) {
                String name = null;
                try {
                    name = in.readSlicedBytesReference().utf8ToString();
                } catch (AssertionError ae) {
                    throw new IllegalArgumentException("Error parsing keyword dimension: " + ae.getMessage(), ae);
                }

                int type = in.read();
                switch (type) {
                    case (byte) 's' -> {
                        // parse a string
                        try {
                            result.put(name, in.readSlicedBytesReference().utf8ToString());
                        } catch (AssertionError ae) {
                            throw new IllegalArgumentException("Error parsing keyword dimension: " + ae.getMessage(), ae);
                        }
                    }
                    case (byte) 'l' -> // parse a long
                        result.put(name, in.readLong());
                    case (byte) 'u' -> { // parse an unsigned_long
                        Object ul = DocValueFormat.UNSIGNED_LONG_SHIFTED.format(in.readLong());
                        result.put(name, ul);
                    }
                    case (byte) 'd' -> // parse a double
                        result.put(name, in.readDouble());
                    case (byte) 'b' -> // parse a boolean
                        result.put(name, in.read() == 't');
                    default -> throw new IllegalArgumentException("Cannot parse [" + name + "]: Unknown type [" + type + "]");
                }
            }
            return result;
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Error formatting " + NAME + ": " + e.getMessage(), e);
        }
    }
}
