/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.hash.MurmurHash3.Hash128;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

/**
 * A mapper for the {@code _id} field that builds the {@code _id} from the
 * {@code _tsid} and {@code @timestamp}.
 */
public class TsidExtractingIdFieldMapper extends IdFieldMapper {
    public static final FieldType FIELD_TYPE = new FieldType();
    /**
     * Maximum length of the {@code _tsid} in the {@link #documentDescription}.
     */
    static final int DESCRIPTION_TSID_LIMIT = 1000;

    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setStored(true);  // TODO reconstruct the id on fetch from tsid and timestamp
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.freeze();
    }

    public static final TsidExtractingIdFieldMapper INSTANCE = new TsidExtractingIdFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(MappingParserContext::idFieldMapper);

    static final class IdFieldType extends TermBasedFieldType {
        IdFieldType() {
            super(NAME, true, true, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            // The _id field is always searchable.
            return true;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return termsQuery(Arrays.asList(value), context);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexed();
            BytesRef[] bytesRefs = values.stream().map(v -> {
                Object idObject = v;
                if (idObject instanceof BytesRef) {
                    idObject = ((BytesRef) idObject).utf8ToString();
                }
                return Uid.encodeId(idObject.toString());
            }).toArray(BytesRef[]::new);
            return new TermInSetQuery(name(), bytesRefs);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("Fielddata is not supported on [_id] field in [time_series] indices");
        }
    }

    private TsidExtractingIdFieldMapper() {
        super(new IdFieldType());
    }

    private static final long SEED = 0;

    public static void createField(DocumentParserContext context, IndexRouting.ExtractFromSource.Builder routingBuilder, BytesRef tsid) {
        IndexableField[] timestampFields = context.rootDoc().getFields(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        if (timestampFields.length == 0) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + DataStreamTimestampFieldMapper.DEFAULT_PATH + "] is missing"
            );
        }
        long timestamp = timestampFields[0].numericValue().longValue();

        Hash128 hash = new Hash128();
        MurmurHash3.hash128(tsid.bytes, tsid.offset, tsid.length, SEED, hash);

        byte[] suffix = new byte[16];
        ByteUtils.writeLongLE(hash.h1, suffix, 0);
        ByteUtils.writeLongBE(timestamp, suffix, 8);   // Big Ending shrinks the inverted index by ~37%

        IndexRouting.ExtractFromSource indexRouting = (IndexRouting.ExtractFromSource) context.indexSettings().getIndexRouting();
        String id = routingBuilder.createId(suffix, () -> {
            if (context.getDynamicMappers().isEmpty() == false) {
                throw new IllegalStateException(
                    "Didn't find any fields to include in the routing which would be fine if there are"
                        + " dynamic mapping waiting but we couldn't find any of those either!"
                );
            }
            return 0;
        });
        assert Uid.isURLBase64WithoutPadding(id); // Make sure we get to use Uid's nice optimizations
        /*
         * Make sure that _id from extracting the tsid matches that _id
         * from extracting the _source. This should be true for all valid
         * documents with valid mappings. *But* some invalid mappings
         * will not parse the field but be rejected later by the dynamic
         * mappings machinery. So if there are any dynamic mappings
         * at all we just skip the assertion because we can't be sure
         * it always must pass.
         */
        assert context.getDynamicMappers().isEmpty() == false
            || context.getDynamicRuntimeFields().isEmpty() == false
            || id.equals(indexRouting.createId(TimeSeriesIdFieldMapper.decodeTsid(tsid), suffix));
        assert context.getDynamicMappers().isEmpty() == false
            || context.getDynamicRuntimeFields().isEmpty() == false
            || id.equals(indexRouting.createId(context.sourceToParse().getXContentType(), context.sourceToParse().source(), suffix));

        if (context.sourceToParse().id() != null && false == context.sourceToParse().id().equals(id)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "_id must be unset or set to [%s] but was [%s] because [%s] is in time_series mode",
                    id,
                    context.sourceToParse().id(),
                    context.indexSettings().getIndexMetadata().getIndex().getName()
                )
            );
        }
        context.id(id);

        BytesRef uidEncoded = Uid.encodeId(context.id());
        context.doc().add(new Field(NAME, uidEncoded, FIELD_TYPE));
    }

    @Override
    public String documentDescription(DocumentParserContext context) {
        /*
         * We don't yet have an _id because it'd be generated by the document
         * parsing process. But we *might* have something more useful - the
         * time series dimensions and the timestamp! If we have those, then
         * include them in the description. If not, all we know is
         * "a time series document".
         */
        StringBuilder description = new StringBuilder("a time series document");
        IndexableField tsidField = context.doc().getField(TimeSeriesIdFieldMapper.NAME);
        if (tsidField != null) {
            description.append(" with dimensions ").append(tsidDescription(tsidField));
        }
        IndexableField timestampField = context.doc().getField(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        if (timestampField != null) {
            String timestamp = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(timestampField.numericValue().longValue());
            description.append(" at [").append(timestamp).append(']');
        }
        return description.toString();
    }

    @Override
    public String documentDescription(ParsedDocument parsedDocument) {
        IndexableField tsidField = parsedDocument.rootDoc().getField(TimeSeriesIdFieldMapper.NAME);
        long timestamp = parsedDocument.rootDoc().getField(DataStreamTimestampFieldMapper.DEFAULT_PATH).numericValue().longValue();
        String timestampStr = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(timestamp);
        return "[" + parsedDocument.id() + "][" + tsidDescription(tsidField) + "@" + timestampStr + "]";
    }

    private static String tsidDescription(IndexableField tsidField) {
        String tsid = TimeSeriesIdFieldMapper.decodeTsid(tsidField.binaryValue()).toString();
        if (tsid.length() <= DESCRIPTION_TSID_LIMIT) {
            return tsid;
        }
        return tsid.substring(0, DESCRIPTION_TSID_LIMIT) + "...}";
    }

    @Override
    public String reindexId(String id) {
        // null the _id so we recalculate it on write
        return null;
    }
}
