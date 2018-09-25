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
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

/**
 * NoOpEngine is an engine implementation that does nothing but the bare minimum
 * required in order to have an engine. All attempts to do something (search,
 * index, get), throw {@link UnsupportedOperationException}. This does maintain
 * a translog with a deletion policy so that when flushing, no translog is
 * retained on disk (setting a retention size and age of 0).
 *
 * It's also important to notice that this does list the commits of the Store's
 * Directory so that the last commit's user data can be read for the historyUUID
 * and last committed segment info.
 */
final class NoOpEngine extends ReadOnlyEngine {

    NoOpEngine(EngineConfig engineConfig) {
        super(engineConfig, null, null, true, directoryReader -> directoryReader);
        boolean success = false;
        try {
            // The deletion policy for the translog should not keep any translogs around, so the min age/size is set to -1
            final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(-1, -1);

            // The translog is opened and closed to validate that the translog UUID from lucene is the same as the one in the translog
            try (Translog translog = openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier())) {
                final int nbOperations = translog.totalOperations();
                if (nbOperations != 0) {
                    throw new IllegalArgumentException("Expected 0 translog operations but there were " + nbOperations);
                }
            }
            success = true;
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    protected DirectoryReader open(final Directory directory) throws IOException {
        final List<IndexCommit> indexCommits = DirectoryReader.listCommits(directory);
        assert indexCommits.size() == 1 : "expected only one commit point";
        IndexCommit indexCommit = indexCommits.get(indexCommits.size() - 1);
        return new DirectoryReader(directory, new LeafReader[0]) {
            @Override
            protected DirectoryReader doOpenIfChanged() throws IOException {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexCommit commit) throws IOException {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
                return null;
            }

            @Override
            public long getVersion() {
                return 0;
            }

            @Override
            public boolean isCurrent() throws IOException {
                return true;
            }

            @Override
            public IndexCommit getIndexCommit() throws IOException {
                return indexCommit;
            }

            @Override
            protected void doClose() throws IOException {
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }

    private Translog openTranslog(EngineConfig engineConfig, TranslogDeletionPolicy translogDeletionPolicy,
                                  LongSupplier globalCheckpointSupplier) throws IOException {
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        final String translogUUID = loadTranslogUUIDFromLastCommit();
        // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
        return new Translog(translogConfig, translogUUID, translogDeletionPolicy, globalCheckpointSupplier,
                engineConfig.getPrimaryTermSupplier());
    }

    /**
     * Reads the current stored translog ID from the last commit data.
     */
    @Nullable
    private String loadTranslogUUIDFromLastCommit() {
        final Map<String, String> commitUserData = getLastCommittedSegmentInfos().getUserData();
        if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY) == false) {
            throw new IllegalStateException("Commit doesn't contain translog generation id");
        }
        return commitUserData.get(Translog.TRANSLOG_UUID_KEY);
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) {
        throw new UnsupportedOperationException("Translog synchronization should never be needed");
    }
}
