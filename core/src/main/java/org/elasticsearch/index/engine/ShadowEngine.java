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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * ShadowEngine is a specialized engine that only allows read-only operations
 * on the underlying Lucene index. An {@code IndexReader} is opened instead of
 * an {@code IndexWriter}. All methods that would usually perform write
 * operations are no-ops, this means:
 *
 * - No operations are written to or read from the translog
 * - Create, Index, and Delete do nothing
 * - Flush does not fsync any files, or make any on-disk changes
 *
 * In order for new segments to become visible, the ShadowEngine may perform
 * stage1 of the traditional recovery process (copying segment files) from a
 * regular primary (which uses {@link org.elasticsearch.index.engine.InternalEngine})
 *
 * Notice that since this Engine does not deal with the translog, any
 * {@link #get(Get get)} request goes directly to the searcher, meaning it is
 * non-realtime.
 */
public class ShadowEngine extends Engine {

    /** how long to wait for an index to exist */
    public final static String NONEXISTENT_INDEX_RETRY_WAIT = "index.shadow.wait_for_initial_commit";
    public final static TimeValue DEFAULT_NONEXISTENT_INDEX_RETRY_WAIT = TimeValue.timeValueSeconds(5);

    private volatile SearcherManager searcherManager;

    private volatile SegmentInfos lastCommittedSegmentInfos;

    public ShadowEngine(EngineConfig engineConfig)  {
        super(engineConfig);
        SearcherFactory searcherFactory = new EngineSearcherFactory(engineConfig);
        final long nonexistentRetryTime = engineConfig.getIndexSettings()
                .getAsTime(NONEXISTENT_INDEX_RETRY_WAIT, DEFAULT_NONEXISTENT_INDEX_RETRY_WAIT)
                .getMillis();
        try {
            DirectoryReader reader = null;
            store.incRef();
            boolean success = false;
            try {
                if (Lucene.waitForIndex(store.directory(), nonexistentRetryTime)) {
                    reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(store.directory()), shardId);
                    this.searcherManager = new SearcherManager(reader, searcherFactory);
                    this.lastCommittedSegmentInfos = readLastCommittedSegmentInfos(searcherManager, store);
                    success = true;
                } else {
                    throw new IllegalStateException("failed to open a shadow engine after" +
                            nonexistentRetryTime + "ms, " +
                            "directory is not an index");
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
        logger.trace("created new ShadowEngine");
    }


    @Override
    public void create(Create create) throws EngineException {
        throw new UnsupportedOperationException(shardId + " create operation not allowed on shadow engine");
    }

    @Override
    public boolean index(Index index) throws EngineException {
        throw new UnsupportedOperationException(shardId + " index operation not allowed on shadow engine");
    }

    @Override
    public void delete(Delete delete) throws EngineException {
        throw new UnsupportedOperationException(shardId + " delete operation not allowed on shadow engine");
    }

    /** @deprecated This was removed, but we keep this API so translog can replay any DBQs on upgrade. */
    @Deprecated
    @Override
    public void delete(DeleteByQuery delete) throws EngineException {
        throw new UnsupportedOperationException(shardId + " delete-by-query operation not allowed on shadow engine");
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) {
        throw new UnsupportedOperationException(shardId + " sync commit operation not allowed on shadow engine");
    }

    @Override
    public CommitId flush() throws EngineException {
        return flush(false, false);
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        logger.trace("skipping FLUSH on shadow engine");
        // reread the last committed segment infos
        refresh("flush");
        /*
         * we have to inc-ref the store here since if the engine is closed by a tragic event
         * we don't acquire the write lock and wait until we have exclusive access. This might also
         * dec the store reference which can essentially close the store and unless we can inc the reference
         * we can't use it.
         */
        store.incRef();
        try (ReleasableLock lock = readLock.acquire()) {
            // reread the last committed segment infos
            lastCommittedSegmentInfos = readLastCommittedSegmentInfos(searcherManager, store);
        } catch (Throwable e) {
            if (isClosed.get() == false) {
                logger.warn("failed to read latest segment infos on flush", e);
                if (Lucene.isCorruptionException(e)) {
                    throw new FlushFailedEngineException(shardId, e);
                }
            }
        } finally {
            store.decRef();
        }
        return new CommitId(lastCommittedSegmentInfos.getId());
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments) throws EngineException {
        // no-op
        logger.trace("skipping FORCE-MERGE on shadow engine");
    }

    @Override
    public GetResult get(Get get) throws EngineException {
        // There is no translog, so we can get it directly from the searcher
        return getFromSearcher(get);
    }

    @Override
    public Translog getTranslog() {
        throw new UnsupportedOperationException("shadow engines don't have translogs");
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        try (ReleasableLock lock = readLock.acquire()) {
            Segment[] segmentsArr = getSegmentInfo(lastCommittedSegmentInfos, verbose);
            for (int i = 0; i < segmentsArr.length; i++) {
                // hard code all segments as committed, because they are in
                // order for the shadow replica to see them
                segmentsArr[i].committed = true;
            }
            return Arrays.asList(segmentsArr);
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            searcherManager.maybeRefreshBlocking();
        } catch (AlreadyClosedException e) {
            ensureOpen();
        } catch (EngineClosedException e) {
            throw e;
        } catch (Throwable t) {
            failEngine("refresh failed", t);
            throw new RefreshFailedEngineException(shardId, t);
        }
    }

    @Override
    public SnapshotIndexCommit snapshotIndex(boolean flushFirst) throws EngineException {
        throw new UnsupportedOperationException("Can not take snapshot from a shadow engine");
    }

    @Override
    protected SearcherManager getSearcherManager() {
        return searcherManager;
    }

    @Override
    protected void closeNoLock(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                logger.debug("shadow replica close searcher manager refCount: {}", store.refCount());
                IOUtils.close(searcherManager);
            } catch (Throwable t) {
                logger.warn("shadow replica failed to close searcher manager", t);
            } finally {
                store.decRef();
            }
        }
    }

    @Override
    public boolean hasUncommittedChanges() {
        return false;
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

}
