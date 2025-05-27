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
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

/**
 * Mapper for {@code _tsid} field included generated when the index is
 * {@link IndexMode#TIME_SERIES organized into time series}.
 */
public class TimeSeriesIdFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_tsid";
    public static final String CONTENT_TYPE = "_tsid";
    public static final TimeSeriesIdFieldType FIELD_TYPE_BYTEREF = new TimeSeriesIdFieldType(false);
    public static final TimeSeriesIdFieldType FIELD_TYPE_LONG = new TimeSeriesIdFieldType(true);

    public static TimeSeriesIdFieldMapper getInstance(MappingParserContext context) {
        boolean useDocValuesSkipper = context.indexVersionCreated().onOrAfter(IndexVersions.TIME_SERIES_ID_DOC_VALUES_SPARSE_INDEX)
            && context.getIndexSettings().useDocValuesSkipper();
        return new TimeSeriesIdFieldMapper(useDocValuesSkipper, context.indexVersionCreated().onOrAfter(IndexVersions.TIME_SERIES_ID_LONG));
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(this.useDocValuesSkipper, this.useIdLong).init(this);
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        private final boolean useDocValuesSkipper;
        private final boolean useIdLong;

        protected Builder(boolean useDocValuesSkipper, boolean useIdLong) {
            super(NAME);
            this.useDocValuesSkipper = useDocValuesSkipper;
            this.useIdLong = useIdLong;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return EMPTY_PARAMETERS;
        }

        @Override
        public TimeSeriesIdFieldMapper build() {
            return new TimeSeriesIdFieldMapper(useDocValuesSkipper, useIdLong);
        }
    }

    public static final TypeParser PARSER = new FixedTypeParser(c -> c.getIndexSettings().getMode().timeSeriesIdFieldMapper(c));

    public static final class TimeSeriesIdFieldType extends MappedFieldType {
        private final boolean useIdLong;

        private TimeSeriesIdFieldType(boolean useIdLong) {
            super(NAME, false, false, true, TextSearchInfo.NONE, Collections.emptyMap());
            this.useIdLong = useIdLong;
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
            checkNoTimeZone(timeZone);
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            if (useIdLong) {
                return (format == null) ? DocValueFormat.RAW : new DocValueFormat.Decimal(format);
            }
            return DocValueFormat.TIME_SERIES_ID;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();

            if (useIdLong) {
                return new SortedNumericIndexFieldData.Builder(
                    name(),
                    IndexNumericFieldData.NumericType.LONG,
                    IndexNumericFieldData.NumericType.LONG.getValuesSourceType(),
                    (dv, n) -> {
                        throw new UnsupportedOperationException();
                    },
                    isIndexed()
                );
            }

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

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return useIdLong
                ? new BlockDocValuesReader.LongsBlockLoader(name())
                : new BlockDocValuesReader.BytesRefsFromOrdsBlockLoader(name());
        }
    }

    private final boolean useDocValuesSkipper;
    private final boolean useIdLong;

    private TimeSeriesIdFieldMapper(boolean useDocValuesSkipper, boolean useIdLong) {
        super(useIdLong ? FIELD_TYPE_LONG : FIELD_TYPE_BYTEREF);
        this.useDocValuesSkipper = useDocValuesSkipper;
        this.useIdLong = useIdLong;
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        assert fieldType().isIndexed() == false;

        final RoutingPathFields routingPathFields = (RoutingPathFields) context.getRoutingFields();
        final BytesRef timeSeriesId;
        if (useIdLong) {
            long hash = routingPathFields.buildLongHash();
            byte[] bytes = new byte[8];
            ByteUtils.writeLongLE(hash, bytes, 0);
            timeSeriesId = new BytesRef(bytes);

            if (this.useDocValuesSkipper) {
                context.doc().add(SortedNumericDocValuesField.indexedField(fieldType().name(), hash));
            } else {
                context.doc().add(new SortedNumericDocValuesField(fieldType().name(), hash));
            }
        } else {
            if (getIndexVersionCreated(context).before(IndexVersions.TIME_SERIES_ID_HASHING)) {
                long limit = context.indexSettings().getValue(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING);
                int size = routingPathFields.routingValues().size();
                if (size > limit) {
                    throw new MapperException("Too many dimension fields [" + size + "], max [" + limit + "] dimension fields allowed");
                }
                timeSeriesId = buildLegacyTsid(routingPathFields).toBytesRef();
            } else {
                timeSeriesId = routingPathFields.buildHash().toBytesRef();
            }

            if (this.useDocValuesSkipper) {
                context.doc().add(SortedDocValuesField.indexedField(fieldType().name(), timeSeriesId));
            } else {
                context.doc().add(new SortedDocValuesField(fieldType().name(), timeSeriesId));
            }
        }

        BytesRef uidEncoded = TsidExtractingIdFieldMapper.createField(
            context,
            getIndexVersionCreated(context).before(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID)
                ? routingPathFields.routingBuilder()
                : null,
            timeSeriesId
        );

        // We need to add the uid or id to nested Lucene documents so that when a document gets deleted, the nested documents are
        // also deleted. Usually this happens when the nested document is created (in DocumentParserContext#createNestedContext), but
        // for time-series indices the _id isn't available at that point.
        for (LuceneDocument doc : context.nonRootDocuments()) {
            assert doc.getField(IdFieldMapper.NAME) == null;
            doc.add(new StringField(IdFieldMapper.NAME, uidEncoded, Field.Store.NO));
        }
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

    public static Object encodeTsid(final BytesRef bytesRef) {
        return base64Encode(bytesRef);
    }

    private static String base64Encode(final BytesRef bytesRef) {
        byte[] bytes = new byte[bytesRef.length];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, bytes, 0, bytesRef.length);
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(bytes);
    }

    public static BytesReference buildLegacyTsid(RoutingPathFields routingPathFields) throws IOException {
        SortedMap<BytesRef, List<BytesReference>> routingValues = routingPathFields.routingValues();
        if (routingValues.isEmpty()) {
            throw new IllegalArgumentException("Dimension fields are missing.");
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(routingValues.size());
            for (var entry : routingValues.entrySet()) {
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

    public interface Loader {
        BytesRef getValue(int docId) throws IOException;

        long last();
    }

    public static Loader getLoader(LeafReader reader, boolean useIdLong) throws IOException {
        return useIdLong ? new LongLoader(reader) : new ByterefLoader(reader);
    }

    static final class ByterefLoader implements Loader {
        private final SortedDocValues tsidDocValues;
        int lastOrdinal;

        ByterefLoader(LeafReader reader) throws IOException {
            this.tsidDocValues = DocValues.getSorted(reader, TimeSeriesIdFieldMapper.NAME);
        }

        @Override
        public BytesRef getValue(int docId) throws IOException {
            boolean found = tsidDocValues.advanceExact(docId);
            assert found;
            lastOrdinal = tsidDocValues.ordValue();
            return tsidDocValues.lookupOrd(lastOrdinal);
        }

        @Override
        public long last() {
            return lastOrdinal;
        }
    }

    static final class LongLoader implements Loader {
        private final SortedNumericDocValues tsidDocValues;
        long lastValue;

        LongLoader(LeafReader reader) throws IOException {
            this.tsidDocValues = DocValues.getSortedNumeric(reader, TimeSeriesIdFieldMapper.NAME);
        }

        @Override
        public BytesRef getValue(int docId) throws IOException {
            boolean found = tsidDocValues.advanceExact(docId);
            assert found;
            lastValue = tsidDocValues.nextValue();
            byte[] bytes = new byte[Long.BYTES];
            ByteUtils.writeLongLE(lastValue, bytes, 0);
            return new BytesRef(bytes);
        }

        @Override
        public long last() {
            return lastValue;
        }
    }
}
