/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
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
import java.util.Collection;
import java.util.Collections;

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

        final RoutingDimensions routingDimensions = (RoutingDimensions) context.getDimensions();
        final BytesRef timeSeriesId;
        if (getIndexVersionCreated(context).before(IndexVersions.TIME_SERIES_ID_HASHING)) {
            long limit = context.indexSettings().getValue(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING);
            int size = routingDimensions.dimensions().size();
            if (size > limit) {
                throw new MapperException("Too many dimension fields [" + size + "], max [" + limit + "] dimension fields allowed");
            }
            timeSeriesId = buildLegacyTsid(routingDimensions).toBytesRef();
        } else {
            timeSeriesId = DimensionHasher.build(routingDimensions).toBytesRef();
        }
        context.doc().add(new SortedDocValuesField(fieldType().name(), timeSeriesId));
        TsidExtractingIdFieldMapper.createField(
            context,
            getIndexVersionCreated(context).before(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID)
                ? routingDimensions.routingBuilder()
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

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }

    public static BytesReference buildLegacyTsid(RoutingDimensions routingDimensions) throws IOException {
        Collection<RoutingDimensions.Dimension> dimensions = routingDimensions.dimensions();
        if (dimensions.isEmpty()) {
            throw new IllegalArgumentException("Dimension fields are missing.");
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(dimensions.size());
            for (RoutingDimensions.Dimension entry : dimensions) {
                out.writeBytesRef(entry.name());
                entry.value().writeTo(out);
            }
            return out.bytes();
        }
    }
}
