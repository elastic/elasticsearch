/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.IntegerDocValuesField;

import java.util.Base64;
import java.util.Collection;
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

    static final class TimeSeriesRoutingIdFieldType extends SimpleMappedFieldType {

        private static final TimeSeriesRoutingIdFieldType INSTANCE = new TimeSeriesRoutingIdFieldType();

        private TimeSeriesRoutingIdFieldType() {
            super(NAME, true, false, true, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return NAME;
        }

        private static int parse(Object value) {
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return Integer.parseInt(value.toString());
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return false;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query termQuery(Object value, @Nullable SearchExecutionContext context) {
            int v = parse(value);
            return IntPoint.newExactQuery(name(), v);
        }

        @Override
        public Query termsQuery(Collection<?> values, @Nullable SearchExecutionContext context) {
            int[] v = values.stream().mapToInt(TimeSeriesRoutingIdFieldType::parse).toArray();
            return IntPoint.newSetQuery(name(), v);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            int l = Integer.MIN_VALUE;
            int u = Integer.MAX_VALUE;
            if (lowerTerm != null) {
                l = parse(lowerTerm);
                if (includeLower == false) {
                    if (l == Integer.MAX_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    ++l;
                }
            }
            if (upperTerm != null) {
                u = parse(upperTerm);
                if (includeUpper == false) {
                    if (u == Integer.MIN_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    --u;
                }
            }
            return IntPoint.newRangeQuery(name(), l, u);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), NumericType.INT, IntegerDocValuesField::new);
        }
    }

    private TimeSeriesRoutingIdFieldMapper() {
        super(TimeSeriesRoutingIdFieldType.INSTANCE);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        if (context.indexSettings().getMode() == IndexMode.TIME_SERIES
            && context.indexSettings().getIndexVersionCreated().onOrAfter(IndexVersions.TIME_SERIES_ROUTING_ID_IN_ID)) {
            int routingId = decode(context.sourceToParse().id());
            var field = new IntField(NAME, routingId, Field.Store.YES);
            context.doc().add(field);
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
        return Base64.getUrlEncoder().encodeToString(bytes);
    }

    public static int decode(String routingId) {
        byte[] bytes = Base64.getUrlDecoder().decode(routingId);
        return ByteUtils.readIntLE(bytes, 0);
    }
}
