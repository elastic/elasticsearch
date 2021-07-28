/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * NoOpEngine is an engine implementation that does nothing but the bare minimum
 * required in order to have an engine. All attempts to do something (search,
 * index, get), throw {@link UnsupportedOperationException}. However, NoOpEngine
 * allows to trim any existing translog files through the usage of the
 * {{@link #trimUnreferencedTranslogFiles()}} method.
 */
public final class NoOpEngine extends ReadOnlyEngine {

    private final SegmentsStats segmentsStats;
    private final DocsStats docsStats;

    public NoOpEngine(EngineConfig config) {
        super(config, null, null, true, Function.identity(), true, true);
        this.segmentsStats = new SegmentsStats();
        Directory directory = store.directory();
        try (DirectoryReader reader = openDirectory(directory)) {
            for (LeafReaderContext ctx : reader.getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                fillSegmentStats(segmentReader, true, segmentsStats);
            }
            this.docsStats = docsStats(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected DirectoryReader open(final IndexCommit commit) throws IOException {
        final Directory directory = commit.getDirectory();
        final List<IndexCommit> indexCommits = DirectoryReader.listCommits(directory);
        final IndexCommit indexCommit = indexCommits.get(indexCommits.size() - 1);
        return new DirectoryReader(directory, new LeafReader[0], null) {
            @Override
            protected DirectoryReader doOpenIfChanged() {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexCommit commit) {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) {
                return null;
            }

            @Override
            public long getVersion() {
                return 0;
            }

            @Override
            public boolean isCurrent() {
                return true;
            }

            @Override
            public IndexCommit getIndexCommit()  {
                return indexCommit;
            }

            @Override
            protected void doClose() {
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        if (includeUnloadedSegments) {
            final SegmentsStats stats = new SegmentsStats();
            stats.add(this.segmentsStats);
            if (includeSegmentFileSizes == false) {
                stats.clearFiles();
            }
            return stats;
        } else {
            return super.segmentsStats(includeSegmentFileSizes, includeUnloadedSegments);
        }
    }

    @Override
    public DocsStats docStats() {
        return docsStats;
    }

    /**
     * This implementation will trim existing translog files using a {@link TranslogDeletionPolicy}
     * that retains nothing but the last translog generation from safe commit.
     */
    @Override
    public void trimUnreferencedTranslogFiles() {
        final Store store = this.engineConfig.getStore();
        store.incRef();
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            final List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
            if (commits.size() == 1 && translogStats.getTranslogSizeInBytes() > translogStats.getUncommittedSizeInBytes()) {
                final Map<String, String> commitUserData = getLastCommittedSegmentInfos().getUserData();
                final String translogUuid = commitUserData.get(Translog.TRANSLOG_UUID_KEY);
                if (translogUuid == null) {
                    throw new IllegalStateException("commit doesn't contain translog unique id");
                }
                final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
                final long localCheckpoint = Long.parseLong(commitUserData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
                final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy();
                translogDeletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpoint);
                try (Translog translog = new Translog(translogConfig, translogUuid, translogDeletionPolicy,
                    engineConfig.getGlobalCheckpointSupplier(), engineConfig.getPrimaryTermSupplier(), seqNo -> {})) {
                    translog.trimUnreferencedReaders();
                    // refresh the translog stats
                    this.translogStats = translog.stats();
                    assert translog.currentFileGeneration() == translog.getMinFileGeneration() : "translog was not trimmed "
                        + " current gen " + translog.currentFileGeneration() + " != min gen " + translog.getMinFileGeneration();
                }
            }
        } catch (final Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog", e);
        } finally {
            store.decRef();
        }
    }
}
