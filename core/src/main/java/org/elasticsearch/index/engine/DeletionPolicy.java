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

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogReader;
import org.elasticsearch.index.translog.TranslogWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

public class DeletionPolicy extends SnapshotDeletionPolicy implements org.elasticsearch.index.translog.TranslogDeletionPolicy {

    public DeletionPolicy() {
        super(new KeepOnlyLastCommitDeletionPolicy());
    }


    /** Records how many views are held against each
     *  translog generation */
    protected final Map<Long,Integer> translogRefCounts = new HashMap<>();

    /**
     * the translog generation that is requires to properly recover from the oldest non deleted
     * {@link org.apache.lucene.index.IndexCommit}.
     */
    private long minTranslogGenerationForRecovery = -1;

    /**
     * a supplier to get the last global checkpoint that is persisted by the translog
     * TODO: remove in first version
     */
    private LongSupplier lastPersistedGlobalCheckpoint;


    @Override
    public synchronized void onInit(List<? extends IndexCommit> commits) throws IOException {
        super.onInit(commits);
        setLastCommittedTranslogGeneration(commits);
    }
    @Override
    public synchronized void onTranslogRollover(List<TranslogReader> readers, TranslogWriter currentWriter) {
        lastPersistedGlobalCheckpoint = currentWriter::lastSyncedGlobalCheckpoint;
    }

    @Override
    public synchronized void onCommit(List<? extends IndexCommit> commits) throws IOException {
        super.onCommit(commits);
        setLastCommittedTranslogGeneration(commits);
    }

    private void setLastCommittedTranslogGeneration(List<? extends IndexCommit> commits) throws IOException {
        // TODO: move this to just use current commit in the first iteration.
        long minGen = Long.MAX_VALUE;
        for (IndexCommit indexCommit : commits) {
            if (indexCommit.isDeleted() == false) {
                long refGen = Long.parseLong(indexCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
                minGen = Math.min(minGen, refGen);
            }
        }
        assert minGen >= minTranslogGenerationForRecovery :
            "a new minTranslogGenerationForRecovery of [" + minGen + "] is lower than the previous one ["
                + minTranslogGenerationForRecovery + "]";
        minTranslogGenerationForRecovery = minGen;
    }

    @Override
    public synchronized long acquireTranslogGenForView() {
        if (lastCommit == null) {
            throw new IllegalStateException("this instance is not being used by IndexWriter; " +
                "be sure to use the instance returned from writer.getConfig().getIndexDeletionPolicy()");
        }
        int current = translogRefCounts.getOrDefault(minTranslogGenerationForRecovery, 0);
       translogRefCounts.put(minTranslogGenerationForRecovery, current + 1);
       return minTranslogGenerationForRecovery;
    }

    @Override
    public synchronized int pendingViewsCount() {
        return translogRefCounts.size();
    }

    @Override
    public synchronized void releaseTranslogGenView(long translogGen) {
        Integer current = translogRefCounts.get(translogGen);
        if (current == null || current <= 0) {
            throw new IllegalArgumentException("translog gen [" + translogGen + "] wasn't acquired");
        }
        if (current == 1) {
            translogRefCounts.remove(translogGen);
        } else {
            translogRefCounts.put(translogGen, current - 1);
        }
    }

    @Override
    public synchronized long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter currentWriter) {
        if (lastCommit == null) {
            throw new IllegalStateException("this instance is not being used by IndexWriter; " +
                "be sure to use the instance returned from writer.getConfig().getIndexDeletionPolicy()");
        }
        // TODO: here we can do things like check for translog size etc.
        long viewRefs = translogRefCounts.keySet().stream().reduce(Math::min).orElse(Long.MAX_VALUE);
        return Math.min(viewRefs, minTranslogGenerationForRecovery);
    }

    /** returns the translog generation that will be used as a basis of a future store/peer recovery */
    @Override
    public long getMinTranslogGenerationForRecovery() {
        if (lastCommit == null) {
            throw new IllegalStateException("this instance is not being used by IndexWriter; " +
                "be sure to use the instance returned from writer.getConfig().getIndexDeletionPolicy()");
        }
        return minTranslogGenerationForRecovery;
    }
}
