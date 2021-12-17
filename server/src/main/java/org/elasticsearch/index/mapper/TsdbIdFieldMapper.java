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
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.mapper.LuceneDocument.DimensionInfo;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * A mapper for the _id field. It does nothing since _id is neither indexed nor
 * stored, but we need to keep it so that its FieldType can be used to generate
 * queries.
 */
public class TsdbIdFieldMapper extends IdFieldMapper {
    public static class Defaults {

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setStored(true);  // TODO reconstruct the id on fetch from tsid and timestamp
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static final TsdbIdFieldMapper INSTANCE = new TsdbIdFieldMapper();

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
    }

    private TsdbIdFieldMapper() {
        super(new IdFieldType(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        int routingHash = 0;
        int nonRoutingHash = 0;
        for (Map.Entry<BytesRef, DimensionInfo> entry : context.doc().getDimensions().entrySet()) {
            int thisHash = hash(entry.getKey()) ^ hash(entry.getValue().tsidBytes());
            if (entry.getValue().isRoutingDimension()) {
                routingHash = 31 * routingHash + thisHash;
            } else {
                nonRoutingHash = 31 * nonRoutingHash + thisHash;
            }
        }
        assert shardFromSource(context.indexSettings().getIndexMetadata(), context.sourceToParse()) == shardFromRoutingHash(
            context.indexSettings().getIndexMetadata(),
            routingHash
        );

        IndexableField[] timestampFields = context.rootDoc().getFields(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        if (timestampFields.length == 0) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + DataStreamTimestampFieldMapper.DEFAULT_PATH + "] is missing"
            );
        }
        long timestamp = timestampFields[0].numericValue().longValue();

        byte[] encoded = new byte[16];
        ByteUtils.writeIntLE(routingHash, encoded, 0);
        ByteUtils.writeIntLE(nonRoutingHash, encoded, 4);
        ByteUtils.writeLongLE(timestamp, encoded, 8);

        /*
         * It'd be more efficient to use the encoded bytes above but everything else
         * assumes that id is a *string*. So we build one. Base 64, url style, without
         * padding. The Uid encoding that the rest of ES wants the id to be treats
         * that format fairly well.
         */
        context.id(Base64.getUrlEncoder().withoutPadding().encodeToString(encoded));
        assert Uid.isURLBase64WithoutPadding(context.id()); // Make
        BytesRef uidEncoded = Uid.encodeId(context.id());
        context.doc().add(new Field(NAME, uidEncoded, Defaults.FIELD_TYPE));
        assert routingHash == decodeRoutingHash(uidEncoded);
    }

    public static int hash(BytesReference value) {
        if (value.hasArray()) {
            return StringHelper.murmurhash3_x86_32(value.array(), value.arrayOffset(), value.length(), 0);
        }
        return hash(value.toBytesRef());
    }

    public static int hash(BytesRef value) {
        return StringHelper.murmurhash3_x86_32(value, 0);
    }

    private static int decodeRoutingHash(BytesRef uidEncodedId) {
        return decodeRoutingHash(Uid.decodeId(uidEncodedId.bytes, uidEncodedId.offset, uidEncodedId.length));
    }

    public static int decodeRoutingHash(String id) {
        byte[] bytes;
        try {
            bytes = Base64.getUrlDecoder().decode(id);
        } catch (IllegalArgumentException e) {
            throw new ResourceNotFoundException("invalid id [{}]", e, id);
        }
        if (bytes.length != 16) {
            throw new ResourceNotFoundException("invalid id [{}]: length was [{}]", id, bytes.length);
        }
        return ByteUtils.readIntLE(bytes, 0);
    }

    private static int shardFromSource(IndexMetadata metadata, SourceToParse sourceToParse) {
        return IndexRouting.fromIndexMetadata(metadata).indexShard(null, null, sourceToParse.getXContentType(), sourceToParse.source());
    }

    private static int shardFromRoutingHash(IndexMetadata metadata, int routingHash) {
        int routingNumShards = metadata.getRoutingNumShards();
        int routingFactor = metadata.getRoutingFactor();
        return Math.floorMod(routingHash, routingNumShards) / routingFactor;
    }
}
