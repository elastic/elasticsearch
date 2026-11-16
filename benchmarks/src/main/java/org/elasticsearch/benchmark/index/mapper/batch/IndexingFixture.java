/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper.batch;

import org.apache.lucene.codecs.Codec;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardBatch;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.benchmark.index.mapper.MapperServiceFactory;
import org.elasticsearch.benchmark.index.mapper.batch.Shapes.BenchShape;
import org.elasticsearch.common.TimeBasedKOrderedUUIDGenerator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Per-trial input for one shape: the mapper service, the codec and the requests to index. Each request's batch is encoded,
 * written through {@code BulkShardBatch.writeTo} and read back, and its items are attached to the result, so the mappers see
 * the batch exactly as a data node does after deserializing a {@code BulkShardRequest}.
 */
public final class IndexingFixture implements Closeable {

    private static final long ID_TIMESTAMP_BASE = 1_704_067_200_000L;
    private static final int ID_TIMESTAMP_JITTER = 86_400_000;
    private static final int ID_DOCS_PER_MILLI = 100;

    private final BenchShape shape;
    private final MapperService mapperService;
    private final IndexSettings indexSettings;
    private final Codec codec;
    private final RequestBatch[] requests;

    private IndexingFixture(BenchShape shape, MapperService mapperService, Codec codec, RequestBatch[] requests) {
        this.shape = shape;
        this.mapperService = mapperService;
        this.indexSettings = mapperService.getIndexSettings();
        this.codec = codec;
        this.requests = requests;
    }

    public record RequestBatch(int offset, BulkItemRequest[] items, BytesReference[] json, BulkShardBatch bulkShardBatch) {

        public SourceBatch batch() {
            return bulkShardBatch.getBatch();
        }

        public int size() {
            return items.length;
        }
    }

    public static IndexingFixture build(BenchShape shape, String sourceMode, int docsPerRequest, int totalDocs, long seed)
        throws IOException {
        if (docsPerRequest < 1 || totalDocs < 1 || totalDocs % docsPerRequest != 0) {
            throw new IllegalArgumentException(
                "docsPerRequest [" + docsPerRequest + "] must be positive and divide totalDocs [" + totalDocs + "]"
            );
        }
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), shape.mode().getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), sourceMode)
            .build();
        MapperService mapperService = MapperServiceFactory.create(shape.mapping(), shape.plugins(), settings);
        List<SourceBatch> parsed = new ArrayList<>();
        boolean success = false;
        try {
            Codec codec = codec(mapperService);
            Random random = new Random(seed);
            String[] ids = seededIds(seed, totalDocs);
            BytesReference[] json = new BytesReference[totalDocs];
            for (int doc = 0; doc < totalDocs; doc++) {
                XContentBuilder b = JsonXContent.contentBuilder();
                b.startObject();
                shape.docs().write(b, random, doc);
                b.endObject();
                json[doc] = BytesReference.bytes(b);
            }
            RequestBatch[] requests = new RequestBatch[totalDocs / docsPerRequest];
            for (int r = 0; r < requests.length; r++) {
                int offset = r * docsPerRequest;
                BytesReference[] sources = Arrays.copyOfRange(json, offset, offset + docsPerRequest);
                BulkItemRequest[] items = new BulkItemRequest[docsPerRequest];
                for (int i = 0; i < docsPerRequest; i++) {
                    items[i] = new BulkItemRequest(
                        i,
                        new IndexRequest("index").id(ids[offset + i]).source(BytesArray.EMPTY, XContentType.JSON)
                    );
                }
                BulkShardBatch bulkShardBatch = wireRoundTrip(sources);
                parsed.add(bulkShardBatch.getBatch());
                BulkShardBatch.attachBatchToItems(bulkShardBatch.getBatch(), items);
                requests[r] = new RequestBatch(offset, items, sources, bulkShardBatch);
            }
            IndexingFixture fixture = new IndexingFixture(shape, mapperService, codec, requests);
            success = true;
            return fixture;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(parsed);
                IOUtils.closeWhileHandlingException(mapperService);
            }
        }
    }

    static String[] seededIds(long seed, int count) {
        Random random = new Random(seed);
        byte[] macAddress = new byte[6];
        random.nextBytes(macAddress);
        int sequenceStart = random.nextInt();
        long timestampStart = ID_TIMESTAMP_BASE + random.nextInt(ID_TIMESTAMP_JITTER);
        long[] next = new long[1];
        TimeBasedKOrderedUUIDGenerator generator = new TimeBasedKOrderedUUIDGenerator(
            () -> timestampStart + next[0]++ / ID_DOCS_PER_MILLI,
            () -> sequenceStart,
            () -> macAddress
        );
        String[] ids = new String[count];
        for (int doc = 0; doc < count; doc++) {
            ids[doc] = generator.getBase64UUID();
        }
        return ids;
    }

    private static Codec codec(MapperService mapperService) {
        String codecName = mapperService.getIndexSettings().getValue(EngineConfig.INDEX_CODEC_SETTING);
        Codec codec = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null).codec(codecName);
        if (CodecService.BEST_COMPRESSION_CODEC.equals(codecName) == false || codec instanceof PerFieldMapperCodec == false) {
            throw new IllegalStateException(
                "expected codec [" + CodecService.BEST_COMPRESSION_CODEC + "] but got [" + codecName + "] -> [" + codec + "]"
            );
        }
        return codec;
    }

    private static BulkShardBatch wireRoundTrip(BytesReference[] sources) throws IOException {
        try (
            EscfBatch encoded = EscfEncoder.encode(Arrays.asList(sources), XContentType.JSON);
            BytesStreamOutput out = new BytesStreamOutput()
        ) {
            new BulkShardBatch(encoded).writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new BulkShardBatch(in);
            }
        }
    }

    public BenchShape shape() {
        return shape;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public IndexSettings indexSettings() {
        return indexSettings;
    }

    public Codec codec() {
        return codec;
    }

    public RequestBatch[] requests() {
        return requests;
    }

    @Override
    public void close() throws IOException {
        List<Closeable> closeables = new ArrayList<>(requests.length + 1);
        for (RequestBatch request : requests) {
            closeables.add(request.batch());
        }
        closeables.add(mapperService);
        IOUtils.close(closeables);
    }
}
