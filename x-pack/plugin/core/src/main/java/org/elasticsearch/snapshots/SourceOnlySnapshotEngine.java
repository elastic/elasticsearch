/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineSearcher;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public final class SourceOnlySnapshotEngine extends Engine {

    private final SegmentInfos lastCommittedSegmentInfos;
    private final SeqNoStats seqNoStats;
    private final TranslogStats translogStats;
    private final SearcherManager searcherManager;
    private final IndexCommit indexCommit;

    public SourceOnlySnapshotEngine(EngineConfig config) {
        super(config);
        try {
            Store store = config.getStore();
            store.incRef();
            DirectoryReader reader = null;
            boolean success = false;
            try {
                this.lastCommittedSegmentInfos = Lucene.readSegmentInfos(store.directory());
                this.translogStats = new TranslogStats(0, 0, 0, 0, 0);
                final SequenceNumbers.CommitInfo seqNoStats =
                    SequenceNumbers.loadSeqNoInfoFromLuceneCommit(lastCommittedSegmentInfos.userData.entrySet());
                long maxSeqNo = seqNoStats.maxSeqNo;
                long localCheckpoint = seqNoStats.localCheckpoint;
                this.seqNoStats = new SeqNoStats(maxSeqNo, localCheckpoint, localCheckpoint);
                reader = SeqIdGeneratingDirectoryReader.wrap(ElasticsearchDirectoryReader.wrap(DirectoryReader
                .open(store.directory()), config.getShardId()), config.getPrimaryTermSupplier().getAsLong());
                this.indexCommit = reader.getIndexCommit();
                this.searcherManager = new SearcherManager(reader, new SearcherFactory());
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(reader, store::decRef);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e); // this is stupid
        }
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                IOUtils.close(searcherManager, store::decRef);
            } catch (Exception ex) {
                logger.warn("failed to close searcher", ex);
            } finally {
                closedLatch.countDown();
            }
        }
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        store.incRef();
        Releasable releasable = store::decRef;
        try (ReleasableLock ignored = readLock.acquire()) {
            final EngineSearcher searcher = new EngineSearcher(source, searcherManager, store, logger);
            releasable = null; // hand over the reference to the engine searcher
            return searcher;
        } catch (AlreadyClosedException ex) {
            throw ex;
        } catch (Exception ex) {
            ensureOpen(ex); // throw AlreadyClosedException if it's closed
            throw new EngineException(shardId, "failed to acquire searcher, source " + source, ex);
        } finally {
            Releasables.close(releasable);
        }
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    public String getHistoryUUID() {
        return lastCommittedSegmentInfos.userData.get(Engine.HISTORY_UUID_KEY);
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public IndexResult index(Index index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult delete(Delete delete) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return false;
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) {
        return false;
    }

    @Override
    public void syncTranslog() {
    }

    @Override
    public Closeable acquireTranslogRetentionLock() {
        return () -> {};
    }

    @Override
    public Translog.Snapshot newTranslogSnapshotFromMinSeqNo(long minSeqNo) {
        return new Translog.Snapshot() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public int totalOperations() {
                return 0;
            }

            @Override
            public Translog.Operation next() throws IOException {
                return null;
            }
        };
    }

    @Override
    public int estimateTranslogOperationsFromMinSeq(long minSeqNo) {
        return 0;
    }

    @Override
    public TranslogStats getTranslogStats() {
        return translogStats;
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLocalCheckpoint() {
        return seqNoStats.getLocalCheckpoint();
    }

    @Override
    public void waitForOpsToComplete(long seqNo) {
    }

    @Override
    public void resetLocalCheckpoint(long newCheckpoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return new SeqNoStats(seqNoStats.getMaxSeqNo(), seqNoStats.getLocalCheckpoint(), globalCheckpoint);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return seqNoStats.getGlobalCheckpoint();
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        return Arrays.asList(getSegmentInfo(lastCommittedSegmentInfos, verbose));
    }

    @Override
    public void refresh(String source) throws EngineException {
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {

    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommitId flush() throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes,
                           boolean upgrade, boolean upgradeOnlyAncientSegments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) {
        store.incRef();
        return new IndexCommitRef(indexCommit, store::decRef);
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() {
        return acquireLastIndexCommit(false);
    }

    @Override
    public void activateThrottling() {
    }

    @Override
    public void deactivateThrottling() {
    }

    @Override
    public void trimUnreferencedTranslogFiles() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restoreLocalCheckpointFromTranslog() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) {
        return 0;
    }

    @Override
    public Engine recoverFromTranslog(long upto) {
        return this;
    }

    @Override
    public void skipTranslogRecovery() {
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {
    }

    @Override
    public void maybePruneDeletes() {
    }



    private static final class SeqIdGeneratingDirectoryReader extends FilterDirectoryReader {
        private final long primaryTerm;

        SeqIdGeneratingDirectoryReader(DirectoryReader in, SeqIdGeneratingSubReaderWrapper wrapper) throws IOException {
            super(in, wrapper);
            primaryTerm = wrapper.primaryTerm;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return wrap(in, primaryTerm);
        }

        public static DirectoryReader wrap(DirectoryReader in, long primaryTerm) throws IOException {
            Map<LeafReader, LeafReaderContext> ctxMap = new IdentityHashMap<>();
            for (LeafReaderContext leave : in.leaves()) {
                ctxMap.put(leave.reader(), leave);
            }
            return new SeqIdGeneratingDirectoryReader(in, new SeqIdGeneratingSubReaderWrapper(ctxMap, primaryTerm));
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        private abstract static class FakeNumericDocValues extends NumericDocValues {
            private final int maxDoc;
            int docID = -1;

            FakeNumericDocValues(int maxDoc) {
                this.maxDoc = maxDoc;
            }

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() {
                if (docID+1 < maxDoc) {
                    docID++;
                } else {
                    docID = NO_MORE_DOCS;
                }
                return docID;
            }

            @Override
            public int advance(int target) {
                if (target >= maxDoc) {
                    docID = NO_MORE_DOCS;
                } else {
                    docID = target;
                }
                return docID;
            }

            @Override
            public long cost() {
                return maxDoc;
            }

            @Override
            public boolean advanceExact(int target) {
                advance(target);
                return docID != NO_MORE_DOCS;
            }
        }

        private static class SeqIdGeneratingSubReaderWrapper extends SubReaderWrapper {
            private final Map<LeafReader, LeafReaderContext> ctxMap;
            private final long primaryTerm;

            SeqIdGeneratingSubReaderWrapper(Map<LeafReader, LeafReaderContext> ctxMap, long primaryTerm) {
                this.ctxMap = ctxMap;
                this.primaryTerm = primaryTerm;
            }

            @Override
            public LeafReader wrap(LeafReader reader) {
                LeafReaderContext leafReaderContext = ctxMap.get(reader);
                final int docBase = leafReaderContext.docBase;
                return new FilterLeafReader(reader) {

                    @Override
                    public NumericDocValues getNumericDocValues(String field) throws IOException {
                        if (SeqNoFieldMapper.NAME.equals(field)) {
                            return new FakeNumericDocValues(maxDoc()) {
                                @Override
                                public long longValue() {
                                    return docBase + docID;
                                }
                            };
                        } else if (SeqNoFieldMapper.PRIMARY_TERM_NAME.equals(field)) {
                            return new FakeNumericDocValues(maxDoc()) {
                                @Override
                                public long longValue() {
                                    return primaryTerm;
                                }
                            };
                        } else if (VersionFieldMapper.NAME.equals(field)) {
                            return new FakeNumericDocValues(maxDoc()) {
                                @Override
                                public long longValue() {
                                    return 1;
                                }
                            };
                        }
                        return super.getNumericDocValues(field);
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return reader.getCoreCacheHelper();
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return reader.getReaderCacheHelper();
                    }
                };
            }
        }
    }
}

