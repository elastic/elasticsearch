/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper.batch;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.benchmark.index.mapper.batch.IndexingFixture.RequestBatch;
import org.elasticsearch.benchmark.index.mapper.batch.LuceneSink.SeqNoStamper;
import org.elasticsearch.benchmark.index.mapper.batch.Shapes.Expect;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.mapper.BatchMappingShardContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.IOException;

/**
 * The columnar batch path without the engine: {@code mapColumnBatch} then {@code IndexWriter.addBatch}. It mirrors
 * {@code ShardBatchIndexer} chunking at {@code BATCH_CHUNK_SIZE} and the seqno, primary term and version stamping
 * {@code InternalEngine} applies to the column slice, so the Lucene input matches production while version map, translog and
 * locking stay out of the measurement.
 */
public final class ColumnIndexer {

    private final BenchShardContext ctx;
    private final Recycler<BytesRef> recycler;

    public ColumnIndexer(BenchShardContext ctx, Recycler<BytesRef> recycler) {
        this.ctx = ctx;
        this.recycler = recycler;
    }

    public record BenchShardContext(MapperService mapperService, IndexSettings indexSettings, long primaryTerm)
        implements
            BatchMappingShardContext {

        @Override
        public long getOperationPrimaryTerm() {
            return primaryTerm;
        }

        @Override
        public long getRelativeTimeInNanos() {
            return System.nanoTime();
        }
    }

    public static final class CountingRecycler implements Recycler<BytesRef> {

        private final Recycler<BytesRef> delegate;
        private long pagesObtained;
        private long pagesFresh;

        public CountingRecycler(Recycler<BytesRef> delegate) {
            this.delegate = delegate;
        }

        public static CountingRecycler pooled() {
            return new CountingRecycler(pooledRecycler());
        }

        @Override
        public V<BytesRef> obtain() {
            V<BytesRef> v = delegate.obtain();
            pagesObtained++;
            if (v.isRecycled() == false) {
                pagesFresh++;
            }
            return v;
        }

        @Override
        public int pageSize() {
            return delegate.pageSize();
        }

        public long pagesObtained() {
            return pagesObtained;
        }

        public long pagesFresh() {
            return pagesFresh;
        }

        public void reset() {
            pagesObtained = 0;
            pagesFresh = 0;
        }
    }

    public static Recycler<BytesRef> pooledRecycler() {
        return new BytesRefRecycler(new PageCacheRecycler(Settings.EMPTY));
    }

    public int index(IndexWriter writer, SeqNoStamper stamper, RequestBatch[] requests) throws IOException {
        int docs = 0;
        for (RequestBatch request : requests) {
            BatchMapperResolution resolution = resolve(ctx, request);
            if (resolution == null) {
                throw new IllegalStateException("resolveMappers refused request at offset [" + request.offset() + "]");
            }
            int size = request.size();
            for (int chunkStart = 0; chunkStart < size; chunkStart += ShardBatchIndexer.BATCH_CHUNK_SIZE) {
                int chunkEnd = Math.min(chunkStart + ShardBatchIndexer.BATCH_CHUNK_SIZE, size);
                docs += indexChunk(writer, stamper, request, chunkStart, chunkEnd, resolution);
            }
        }
        return docs;
    }

    private int indexChunk(
        IndexWriter writer,
        SeqNoStamper stamper,
        RequestBatch request,
        int chunkStart,
        int chunkEnd,
        BatchMapperResolution resolution
    ) throws IOException {
        try (
            EngineBatch engineBatch = ShardBatchMapper.mapColumnBatch(
                request.items(),
                request.batch(),
                ctx,
                chunkStart,
                chunkEnd,
                resolution,
                Engine.Operation.Origin.PRIMARY,
                recycler
            )
        ) {
            if (engineBatch == null) {
                throw new IllegalStateException(
                    "mapColumnBatch fell back on request at offset [" + request.offset() + "] chunk [" + chunkStart + ", " + chunkEnd + ")"
                );
            }
            int n = chunkEnd - chunkStart;
            MappedColumns slice = engineBatch.columns().slice(0, n);
            slice.fillPrimaryTerm(stamper.primaryTerm());
            for (int i = 0; i < n; i++) {
                slice.setSeqNo(i, stamper.nextSeqNo());
                slice.setVersion(i, stamper.version());
            }
            writer.addBatch(slice.toColumnBatch());
            return n;
        }
    }

    public static BatchMapperResolution resolve(BenchShardContext ctx, RequestBatch request) {
        return ShardBatchMapper.resolveMappers(request.batch(), ctx.mapperService().mappingLookup(), ctx.indexSettings());
    }

    public static Expect dryRunStage(BenchShardContext ctx, RequestBatch request) {
        BatchMapperResolution resolution = resolve(ctx, request);
        if (resolution == null) {
            return Expect.FALLBACK_RESOLVE;
        }
        return dryRunMaps(ctx, request, resolution, BytesRefRecycler.NON_RECYCLING_INSTANCE) ? Expect.COLUMN : Expect.FALLBACK_MAP;
    }

    public static boolean dryRunMaps(
        BenchShardContext ctx,
        RequestBatch request,
        BatchMapperResolution resolution,
        Recycler<BytesRef> recycler
    ) {
        try (
            EngineBatch engineBatch = ShardBatchMapper.mapColumnBatch(
                request.items(),
                request.batch(),
                ctx,
                0,
                Math.min(ShardBatchIndexer.BATCH_CHUNK_SIZE, request.size()),
                resolution,
                Engine.Operation.Origin.PRIMARY,
                recycler
            )
        ) {
            return engineBatch != null;
        }
    }
}
