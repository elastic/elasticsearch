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
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexMode;
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
    public static final TimeSeriesIdFieldType FIELD_TYPE = new TimeSeriesIdFieldType();

    private static final TimeSeriesIdFieldMapper INSTANCE_WITHOUT_SKIPPER = new TimeSeriesIdFieldMapper(false);
    private static final TimeSeriesIdFieldMapper INSTANCE_WITH_SKIPPER = new TimeSeriesIdFieldMapper(true);

    public static TimeSeriesIdFieldMapper getInstance(boolean useDocValuesSkipper) {
        if (useDocValuesSkipper) {
            return INSTANCE_WITH_SKIPPER;
        } else {
            return INSTANCE_WITHOUT_SKIPPER;
        }
    }

    public static TimeSeriesIdFieldMapper getInstance(MappingParserContext context) {
        boolean useDocValuesSkipper = context.indexVersionCreated().onOrAfter(IndexVersions.TIME_SERIES_ID_DOC_VALUES_SPARSE_INDEX)
            && context.getIndexSettings().useDocValuesSkipper();
        return TimeSeriesIdFieldMapper.getInstance(useDocValuesSkipper);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(this.useDocValuesSkipper).init(this);
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        private final boolean useDocValuesSkipper;

        protected Builder(boolean useDocValuesSkipper) {
            super(NAME);
            this.useDocValuesSkipper = useDocValuesSkipper;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return EMPTY_PARAMETERS;
        }

        @Override
        public TimeSeriesIdFieldMapper build() {
            return TimeSeriesIdFieldMapper.getInstance(useDocValuesSkipper);
        }
    }

    public static final TypeParser PARSER = new FixedTypeParser(c -> c.getIndexSettings().getMode().timeSeriesIdFieldMapper(c));

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

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return new BlockDocValuesReader.BytesRefsFromOrdsBlockLoader(name());
        }
    }

    private final boolean useDocValuesSkipper;

    private TimeSeriesIdFieldMapper(boolean useDocValuesSkipper) {
        super(FIELD_TYPE);
        this.useDocValuesSkipper = useDocValuesSkipper;
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        assert fieldType().isIndexed() == false;

        final RoutingPathFields routingPathFields = (RoutingPathFields) context.getRoutingFields();
        final BytesRef timeSeriesId;
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
}
