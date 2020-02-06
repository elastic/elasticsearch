/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    private final SegmentsStats stats;

    public NoOpEngine(EngineConfig config) {
        super(config, null, null, true, Function.identity());
        this.stats = new SegmentsStats();
        Directory directory = store.directory();
        // Do not wrap soft-deletes reader when calculating segment stats as the wrapper might filter out fully deleted segments.
        try (DirectoryReader reader = openDirectory(directory, false)) {
            for (LeafReaderContext ctx : reader.getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                fillSegmentStats(segmentReader, true, stats);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected DirectoryReader open(final IndexCommit commit) throws IOException {
        final Directory directory = commit.getDirectory();
        final List<IndexCommit> indexCommits = DirectoryReader.listCommits(directory);
        final IndexCommit indexCommit = indexCommits.get(indexCommits.size() - 1);
        return new DirectoryReader(directory, new LeafReader[0]) {
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
            stats.add(this.stats);
            if (includeSegmentFileSizes == false) {
                stats.clearFileSizes();
            }
            return stats;
        } else {
            return super.segmentsStats(includeSegmentFileSizes, includeUnloadedSegments);
        }
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
            if (commits.size() == 1) {
                final Map<String, String> commitUserData = getLastCommittedSegmentInfos().getUserData();
                final String translogUuid = commitUserData.get(Translog.TRANSLOG_UUID_KEY);
                if (translogUuid == null) {
                    throw new IllegalStateException("commit doesn't contain translog unique id");
                }
                if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY) == false) {
                    throw new IllegalStateException("commit doesn't contain translog generation id");
                }
                final long lastCommitGeneration = Long.parseLong(commitUserData.get(Translog.TRANSLOG_GENERATION_KEY));
                final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
                final long minTranslogGeneration = Translog.readMinTranslogGeneration(translogConfig.getTranslogPath(), translogUuid);

                if (minTranslogGeneration < lastCommitGeneration) {
                    // a translog deletion policy that retains nothing but the last translog generation from safe commit
                    final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy();
                    translogDeletionPolicy.setTranslogGenerationOfLastCommit(lastCommitGeneration);
                    translogDeletionPolicy.setMinTranslogGenerationForRecovery(lastCommitGeneration);

                    try (Translog translog = new Translog(translogConfig, translogUuid, translogDeletionPolicy,
                        engineConfig.getGlobalCheckpointSupplier(), engineConfig.getPrimaryTermSupplier(), seqNo -> {})) {
                        translog.trimUnreferencedReaders();
                        // refresh the translog stats
                        this.translogStats = translog.stats();
                    }
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
