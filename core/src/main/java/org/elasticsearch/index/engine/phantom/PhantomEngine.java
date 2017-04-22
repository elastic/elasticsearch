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

package org.elasticsearch.index.engine.phantom;

import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.WaitingSearcherManager;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Phantom engine can unload shard data and load it back.
 * It does not allow modification of underlying shard data, so:
 * <ul>
 *     <li>returns cached stats when request to avoid data load process;
 *     <li>entire index should be read-only.
 * </ul>
 * But still, this engine allows to freely move their shard to another node.
 * <p>
 * Load/unload methods are not public because {@link PhantomEnginesManager}
 * controls loading process across all existing engines.
 *
 * @author ikuznetsov
 */
public class PhantomEngine extends Engine {

    private static final String PHANTOM_ENGINE = "phantom_engine";

    private final SegmentInfos lastCommittedSegmentInfos;
    private final DocsStats docsStats;
    private final SegmentsStats segmentsStats;
    private final ReentrantLock lock = new ReentrantLock();
    private final PhantomEnginesManager manager;

    private final AtomicLong loadCount = new AtomicLong(0);
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong totalHits = new AtomicLong(0);
    private final AtomicLong loadTimestamp = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong loadTime = new AtomicLong(0);
    private final AtomicLong unloadTime = new AtomicLong(0);
    private final AtomicLong searchCompletionAwaitTime = new AtomicLong(0);
    private final ByteSizeValue usedHeapSize;
    private final long segmentCount;

    private volatile WaitingSearcherManager searcherManager;
    private DirectoryReader reader;

    @Inject
    public PhantomEngine(PhantomEnginesManager manager, @Assisted EngineConfig engineConfig) {
        super(engineConfig);
        this.manager = manager;

        long usedHeapSize = 0;
        long segmentCount = 0;

        try {
            load();

            lastCommittedSegmentInfos = readLastCommittedSegmentInfos(searcherManager, store);

            IndexSearcher indexSearcher = searcherManager.acquire();
            Searcher searcher = new EngineSearcher(PHANTOM_ENGINE, indexSearcher, searcherManager, store, logger);

            IndexCommit indexCommit = searcher.getDirectoryReader().getIndexCommit();
            deletionPolicy.onCommit(Collections.singletonList(indexCommit));

            docsStats = new DocsStats(searcher.reader().numDocs(), searcher.reader().numDeletedDocs());

            segmentsStats = new SegmentsStats();
            for (LeafReaderContext reader : searcher.reader().leaves()) {
                usedHeapSize += segmentReader(reader.reader()).ramBytesUsed();
                segmentCount++;

                final SegmentReader segmentReader = segmentReader(reader.reader());
                segmentsStats.add(1, segmentReader.ramBytesUsed());
                segmentsStats.addTermsMemoryInBytes(guardedRamBytesUsed(segmentReader.getPostingsReader()));
                segmentsStats.addStoredFieldsMemoryInBytes(guardedRamBytesUsed(segmentReader.getFieldsReader()));
                segmentsStats.addTermVectorsMemoryInBytes(guardedRamBytesUsed(segmentReader.getTermVectorsReader()));
                segmentsStats.addNormsMemoryInBytes(guardedRamBytesUsed(segmentReader.getNormsReader()));
                segmentsStats.addDocValuesMemoryInBytes(guardedRamBytesUsed(segmentReader.getDocValuesReader()));
            }

            searcherManager.release(indexSearcher);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.usedHeapSize = ByteSizeValue.of(usedHeapSize);
        this.segmentCount = segmentCount;

        manager.add(this);

        if (logger.isDebugEnabled()) {
            logger.debug("loaded phantom engine for {}, heap size [{}], segment count [{}], doc count [{}]",
                shardId, this.usedHeapSize, this.segmentCount, docsStats.getCount());
        }
    }

    void load() {
        if (searcherManager != null) {
            return;
        }

        final long nonexistentRetryTime = engineConfig.getIndexSettings()
            .getAsTime(ShadowEngine.NONEXISTENT_INDEX_RETRY_WAIT, ShadowEngine.DEFAULT_NONEXISTENT_INDEX_RETRY_WAIT)
            .getMillis();

        logger.debug("opening searcher manager, ref count: {}", store.refCount());

        long startTime = System.nanoTime();

        try {
            store.incRef();
            boolean success = false;
            try {
                if (Lucene.waitForIndex(store.directory(), nonexistentRetryTime)) {

                    this.reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(store.directory()), shardId);
                    this.searcherManager = new WaitingSearcherManager(reader, new EngineSearcherFactory(engineConfig));

                    logger.debug("opened searcher manager, ref count: {}", store.refCount());
                    loadTimestamp.set(System.currentTimeMillis());
                    loadCount.incrementAndGet();
                    loadTime.addAndGet((long) ((System.nanoTime() - startTime) / 1e6));
                    success = true;
                } else {
                    String msg = "failed to open a phantom engine after" +
                        nonexistentRetryTime + "ms, directory is not an index";
                    throw new IllegalStateException(msg);
                }
            } catch (Throwable e) {
                logger.warn("failed to create new reader", e);
                throw e;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(reader);
                    store.decRef();
                }
            }
        } catch (IOException ex) {
            throw new EngineCreationFailureException(shardId, "failed to open index reader", ex);
        }
    }

    void unload() {
        if (searcherManager == null) {
            return;
        }

        lock.lock();

        long startTime = System.nanoTime();

        try {
            long time = searcherManager.awaitSearchCompletion(logger);
            searchCompletionAwaitTime.addAndGet(time);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {

            logger.debug("closing searcher manager, ref count: {}", store.refCount());
            IOUtils.close(searcherManager);
            logger.debug("closed searcher manager");

        } catch (Throwable t) {
            logger.warn("failed to close searcher manager", t);
        } finally {
            searcherManager = null;
            reader = null;
            store.decRef();
            hits.set(0);
            unloadTime.addAndGet((long) ((System.nanoTime() - startTime) / 1e6));
            logger.debug("closed searcher manager, ref count: {}", store.refCount());

            lock.unlock();
        }

        if (store.refCount() < 1) {
            throw new RuntimeException("Extra close?");
        }
    }

    public DocsStats docsStats() {
        return docsStats;
    }

    @Override
    public SegmentsStats segmentsStats() {
        return segmentsStats;
    }

    public ShardId shardId() {
        return shardId;
    }

    public boolean hasActiveSearches() {
        return searcherManager != null && searcherManager.hasActiveSearches();
    }

    public long hits() {
        return hits.get();
    }

    public long totalHits() {
        return totalHits.get();
    }

    public long loadCount() {
        return loadCount.get();
    }

    public long loadTimestamp() {
        return loadTimestamp.get();
    }

    public long loadTime() {
        return loadTime.get();
    }

    public long unloadTime() {
        return unloadTime.get();
    }

    public long searchCompletionAwaitTime() {
        return searchCompletionAwaitTime.get();
    }

    public long usedHeapSizeInBytes() {
        return usedHeapSize.bytes();
    }

    public ByteSizeValue usedHeapSize() {
        return usedHeapSize;
    }

    public long segmentCount() {
        return segmentCount;
    }

    /**
     *
     * @return true - loaded, false - unloaded or closed
     */
    public boolean loadState() {
        return searcherManager != null;
    }

    @Override
    protected void closeNoLock(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            unload();
            manager.remove(this);
        }
    }

    @Override
    public GetResult get(Get get) throws EngineException {
        return getFromSearcher(get);
    }

    @Override
    protected Searcher acquireSearcher(String source, boolean maybeWrap) throws EngineException {
        if ("search".equals(source)) {
            hits.incrementAndGet();
            totalHits.incrementAndGet();
        }

        // Unload may be triggered between getSearcherManager() call
        // and acquire() on obtained manager (see overridden method),
        // so do acquiring under lock.
        // acquire() on manager increments it's ref counter,
        // so unload() call proceed to wait as expected
        lock.lock();
        try {
            return super.acquireSearcher(source, maybeWrap);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected WaitingSearcherManager getSearcherManager() {
        manager.add(this); // load() will be called there
        return searcherManager;
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    public void create(Create create) throws EngineException {
        throw new UnsupportedOperationException(shardId + " create operation not allowed on phantom engine");
    }

    @Override
    public boolean index(Index index) throws EngineException {
        throw new UnsupportedOperationException(shardId + " index operation not allowed on phantom engine");
    }

    @Override
    public void delete(Delete delete) throws EngineException {
        throw new UnsupportedOperationException(shardId + " delete operation not allowed on phantom engine");
    }

    @Override
    public void delete(DeleteByQuery delete) throws EngineException {
        throw new UnsupportedOperationException(shardId + " delete-by-query operation not allowed on phantom engine");
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        // best effort attempt before we acquire locks
        ensureOpen();
        if (expectedCommitId.idsEqual(lastCommittedSegmentInfos.getId()) == false) {
            logger.trace("can't sync commit [{}]. current commit id is not equal to expected.", syncId);
            return SyncedFlushResult.COMMIT_MISMATCH;
        }
        return SyncedFlushResult.SUCCESS;
    }

    @Override
    public Translog getTranslog() {
        // Nothing to store into translog
        return null;
    }

    @Override
    public long indexWriterRAMBytesUsed() {
        // No IndexWriter
        throw new UnsupportedOperationException("PhantomEngine has no IndexWriter");
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        return Collections.emptyList();
    }

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    @Override
    public void refresh(String source) throws EngineException {

    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        logger.trace("skipping FLUSH on phantom engine");
        // reread the last committed segment infos
        return new CommitId(new byte[0]);
    }

    @Override
    public CommitId flush() throws EngineException {
        return flush(false, false);
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments) throws EngineException, IOException {
        logger.trace("skipping FORCE-MERGE on phantom engine");
    }

    @Override
    public SnapshotIndexCommit snapshotIndex(boolean flushFirst) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            logger.trace("pulling snapshot");
            return deletionPolicy.snapshot();
        } catch (IOException e) {
            throw new SnapshotFailedEngineException(shardId, e);
        }
    }

    @Override
    public boolean hasUncommittedChanges() {
        return false;
    }
}
