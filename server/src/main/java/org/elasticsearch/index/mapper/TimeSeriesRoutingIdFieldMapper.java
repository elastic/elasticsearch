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
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.util.Base64;
import java.util.Collections;

/**
 * Mapper for the {@code _seq_no} field.
 *
 * We expect to use the seq# for sorting, during collision checking and for
 * doing range searches. Therefore the {@code _seq_no} field is stored both
 * as a numeric doc value and as numeric indexed field.
 *
 * This mapper also manages the primary term field, which has no ES named
 * equivalent. The primary term is only used during collision after receiving
 * identical seq# values for two document copies. The primary term is stored as
 * a doc value field without being indexed, since it is only intended for use
 * as a key-value lookup.

 */
public class TimeSeriesRoutingIdFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_routing_id";

    public static final TimeSeriesRoutingIdFieldMapper INSTANCE = new TimeSeriesRoutingIdFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    static final class TimeSeriesRoutingIdFieldType extends MappedFieldType {

        private static final TimeSeriesRoutingIdFieldType INSTANCE = new TimeSeriesRoutingIdFieldType();

        private TimeSeriesRoutingIdFieldType() {
            super(NAME, false, false, true, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this, MappedFieldType.FielddataOperation.SEARCH));
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
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

    private TimeSeriesRoutingIdFieldMapper() {
        super(TimeSeriesRoutingIdFieldType.INSTANCE);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        if (context.indexSettings().getMode() == IndexMode.TIME_SERIES
            && context.indexSettings().getIndexVersionCreated().onOrAfter(IndexVersions.TIME_SERIES_ROUTING_ID_IN_ID)
            && context.sourceToParse().id() != null) {
            var field = new SortedDocValuesField(NAME, Uid.encodeId(context.sourceToParse().id()));
            context.rootDoc().add(field);
        }
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }

    public static String encode(int routingId) {
        byte[] bytes = new byte[4];
        ByteUtils.writeIntLE(routingId, bytes, 0);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    public static int decode(String routingId) {
        byte[] bytes = Base64.getUrlDecoder().decode(routingId);
        return ByteUtils.readIntLE(bytes, 0);
    }
}
