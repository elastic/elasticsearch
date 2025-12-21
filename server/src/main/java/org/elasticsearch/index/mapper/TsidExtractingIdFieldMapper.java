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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingHashBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.hash.MurmurHash3.Hash128;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.util.Locale;

/**
 * A mapper for the {@code _id} field that builds the {@code _id} from the
 * {@code _tsid} and {@code @timestamp}.
 */
public class TsidExtractingIdFieldMapper extends IdFieldMapper {
    /**
     * Maximum length of the {@code _tsid} in the {@link #documentDescription}.
     */
    static final int DESCRIPTION_TSID_LIMIT = 1000;

    public static final TsidExtractingIdFieldMapper INSTANCE = new TsidExtractingIdFieldMapper();

    private TsidExtractingIdFieldMapper() {
        super(new AbstractIdFieldType() {
            @Override
            public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
                throw new IllegalArgumentException("Fielddata is not supported on [_id] field in [time_series] indices");
            }
        });
    }

    private static final long SEED = 0;

    public static BytesRef createField(DocumentParserContext context, RoutingHashBuilder routingBuilder, BytesRef tsid) {
        final long timestamp = DataStreamTimestampFieldMapper.extractTimestampValue(context.doc());
        String id;
        if (routingBuilder != null) {
            byte[] suffix = new byte[16];
            id = createId(context.hasDynamicMappers(), routingBuilder, tsid, timestamp, suffix);
            /*
             * Make sure that _id from extracting the tsid matches that _id
             * from extracting the _source. This should be true for all valid
             * documents with valid mappings. *But* some invalid mappings
             * will not parse the field but be rejected later by the dynamic
             * mappings machinery. So if there are any dynamic mappings
             * at all we just skip the assertion because we can't be sure
             * it always must pass.
             */
            var indexRouting = (IndexRouting.ExtractFromSource.ForRoutingPath) context.indexSettings().getIndexRouting();
            assert context.getDynamicMappers().isEmpty() == false
                || context.getDynamicRuntimeFields().isEmpty() == false
                || id.equals(indexRouting.createId(context.sourceToParse().getXContentType(), context.sourceToParse().source(), suffix));
        } else if (context.sourceToParse().routing() != null) {
            int routingHash = TimeSeriesRoutingHashFieldMapper.decode(context.sourceToParse().routing());
            if (context.indexSettings().useTimeSeriesSyntheticId()) {
                id = createSyntheticId(tsid, timestamp, routingHash);
            } else {
                id = createId(routingHash, tsid, timestamp);
            }
        } else {
            if (context.sourceToParse().id() == null) {
                throw new IllegalArgumentException(
                    "_ts_routing_hash was null but must be set because index ["
                        + context.indexSettings().getIndexMetadata().getIndex().getName()
                        + "] is in time_series mode"
                );
            }
            // In Translog operations, the id has already been generated based on the routing hash while the latter is no longer available.
            id = context.sourceToParse().id();
        }
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
        assert id != null;
        context.id(id);

        final Field idField;
        if (context.indexSettings().useTimeSeriesSyntheticId()) {
            idField = syntheticIdField(context.id());
        } else {
            idField = standardIdField(context.id());
        }
        assert NAME.equals(idField.name()) : idField.name();
        assert idField.binaryValue() != null;

        context.doc().add(idField);
        return idField.binaryValue();
    }

    public static String createId(int routingHash, BytesRef tsid, long timestamp) {
        Hash128 hash = new Hash128();
        MurmurHash3.hash128(tsid.bytes, tsid.offset, tsid.length, SEED, hash);

        byte[] bytes = new byte[20];
        ByteUtils.writeIntLE(routingHash, bytes, 0);
        ByteUtils.writeLongLE(hash.h1, bytes, 4);
        ByteUtils.writeLongBE(timestamp, bytes, 12);   // Big Ending shrinks the inverted index by ~37%

        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(bytes);
    }

    public static long extractTimestampFromId(byte[] id) {
        assert id.length == 20;
        // id format: [4 bytes (basic hash routing fields), 8 bytes prefix of 128 murmurhash dimension fields, 8 bytes
        // @timestamp)
        return ByteUtils.readLongBE(id, 12);
    }

    public static String createId(
        boolean dynamicMappersExists,
        RoutingHashBuilder routingBuilder,
        BytesRef tsid,
        long timestamp,
        byte[] suffix
    ) {
        Hash128 hash = new Hash128();
        MurmurHash3.hash128(tsid.bytes, tsid.offset, tsid.length, SEED, hash);

        ByteUtils.writeLongLE(hash.h1, suffix, 0);
        ByteUtils.writeLongBE(timestamp, suffix, 8);   // Big Ending shrinks the inverted index by ~37%

        String id = routingBuilder.createId(suffix, dynamicMappersExists ? () -> 0 : () -> {
            throw new IllegalStateException(
                "Didn't find any fields to include in the routing which would be fine if there are"
                    + " dynamic mapping waiting but we couldn't find any of those either!"
            );
        });
        assert Uid.isURLBase64WithoutPadding(id); // Make sure we get to use Uid's nice optimizations
        return id;
    }

    public static BytesRef createSyntheticIdBytesRef(BytesRef tsid, long timestamp, int routingHash) {
        // A synthetic _id has the format: [_tsid (non-fixed length) + (Long.MAX_VALUE - timestamp) (8 bytes) + routing hash (4 bytes)].
        // We dont' use hashing here because we need to be able to extract the concatenated values from the _id in various places, like
        // when applying doc values updates in Lucene, or when routing GET or DELETE requests to the corresponding shard, or when replaying
        // translog operations. Since the synthetic _id is not indexed and not really stored on disk we consider it fine if it is longer
        // that standard ids.
        //
        // Also, when applying doc values updates Lucene expects _id to be sorted: it stops applying updates for a term "_id:ABC" if it
        // seeks to a term "BCD" as it knows there won't be more documents matching "_id:ABC" past the term "BCD". So it is important to
        // generate an _id as a byte array whose lexicographical order reflects the order of the documents in the segment. For this reason,
        // the timestamp is stored in the synthetic _id as (Long.MAX_VALUE - timestamp).
        byte[] bytes = new byte[tsid.length + Long.BYTES + Integer.BYTES];
        System.arraycopy(tsid.bytes, tsid.offset, bytes, 0, tsid.length);
        ByteUtils.writeLongBE(Long.MAX_VALUE - timestamp, bytes, tsid.length); // Big Endian as we want to most significant byte first
        ByteUtils.writeIntBE(routingHash, bytes, tsid.length + Long.BYTES);
        return new BytesRef(bytes);
    }

    public static String createSyntheticId(BytesRef tsid, long timestamp, int routingHash) {
        BytesRef id = createSyntheticIdBytesRef(tsid, timestamp, routingHash);
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(id.bytes);
    }

    public static BytesRef extractTimeSeriesIdFromSyntheticId(BytesRef id) {
        assert id.length > Long.BYTES + Integer.BYTES;
        // See #createSyntheticId
        byte[] tsId = new byte[Math.toIntExact(id.length - Long.BYTES - Integer.BYTES)];
        System.arraycopy(id.bytes, id.offset, tsId, 0, tsId.length);
        return new BytesRef(tsId);
    }

    public static long extractTimestampFromSyntheticId(BytesRef id) {
        assert id.length > Long.BYTES + Integer.BYTES;
        // See #createSyntheticId
        long delta = ByteUtils.readLongBE(id.bytes, id.offset + id.length - Long.BYTES - Integer.BYTES);
        long timestamp = Long.MAX_VALUE - delta;
        assert timestamp >= 0 : delta;
        return timestamp;
    }

    public static int extractRoutingHashFromSyntheticId(BytesRef id) {
        assert id.length > Long.BYTES + Integer.BYTES;
        // See #createSyntheticId
        return ByteUtils.readIntBE(id.bytes, id.offset + id.length - Integer.BYTES);
    }

    public static BytesRef extractRoutingHashBytesFromSyntheticId(BytesRef id) {
        int hash = extractRoutingHashFromSyntheticId(id);
        return Uid.encodeId(TimeSeriesRoutingHashFieldMapper.encode(hash));
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
            description.append(" with tsid ").append(tsidDescription(tsidField));
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
        String tsid = TimeSeriesIdFieldMapper.encodeTsid(tsidField.binaryValue()).toString();
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
