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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexMode;
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

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

/**
 * Mapper for the {@code _ts_routing_hash} field.
 *
 * The field contains the routing hash, as calculated in coordinating nodes for docs in time-series indexes.
 * It's stored to be retrieved and added as a prefix when reconstructing the _id field in search queries.
 * The prefix can then used for routing Get and Delete requests (by doc id) to the right shard.
 */
public class TimeSeriesRoutingHashFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_ts_routing_hash";

    public static final TimeSeriesRoutingHashFieldMapper INSTANCE = new TimeSeriesRoutingHashFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> c.getIndexSettings().getMode().timeSeriesRoutingHashFieldMapper());
    static final NodeFeature TS_ROUTING_HASH_FIELD_PARSES_BYTES_REF = new NodeFeature("tsdb.ts_routing_hash_doc_value_parse_byte_ref");

    public static DocValueFormat TS_ROUTING_HASH_DOC_VALUE_FORMAT = TimeSeriesRoutingHashFieldType.DOC_VALUE_FORMAT;

    static final class TimeSeriesRoutingHashFieldType extends MappedFieldType {

        private static final TimeSeriesRoutingHashFieldType INSTANCE = new TimeSeriesRoutingHashFieldType();
        static final DocValueFormat DOC_VALUE_FORMAT = new DocValueFormat() {

            @Override
            public String getWriteableName() {
                return NAME;
            }

            @Override
            public void writeTo(StreamOutput out) {}

            @Override
            public Object format(BytesRef value) {
                return Uid.decodeId(value.bytes, value.offset, value.length);
            }

            @Override
            public BytesRef parseBytesRef(Object value) {
                if (value instanceof BytesRef valueAsBytesRef) {
                    return valueAsBytesRef;
                }
                return Uid.encodeId(value.toString());
            }
        };

        private TimeSeriesRoutingHashFieldType() {
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

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DOC_VALUE_FORMAT;
        }
    }

    private TimeSeriesRoutingHashFieldMapper() {
        super(TimeSeriesRoutingHashFieldType.INSTANCE);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        if (context.indexSettings().getMode() == IndexMode.TIME_SERIES
            && context.indexSettings().getIndexVersionCreated().onOrAfter(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID)) {
            String routingHash = context.sourceToParse().routing();
            if (routingHash == null) {
                assert context.sourceToParse().id() != null;
                routingHash = Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(
                    Arrays.copyOf(Base64.getUrlDecoder().decode(context.sourceToParse().id()), 4)
                );
            }
            var field = new SortedDocValuesField(NAME, Uid.encodeId(routingHash));
            context.rootDoc().add(field);
        }
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    public static String encode(int routingId) {
        byte[] bytes = new byte[4];
        ByteUtils.writeIntLE(routingId, bytes, 0);
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(bytes);
    }

    public static final String DUMMY_ENCODED_VALUE = encode(0);

    public static int decode(String routingId) {
        byte[] bytes = Base64.getUrlDecoder().decode(routingId);
        return ByteUtils.readIntLE(bytes, 0);
    }
}
