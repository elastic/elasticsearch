/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.CloseableComponent;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadSafe;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.translog.Translog;

import javax.annotation.Nullable;

/**
 * @author kimchy (shay.banon)
 */
@ThreadSafe
public interface Engine extends IndexShardComponent, CloseableComponent {

    void indexingBuffer(ByteSizeValue indexingBufferSize);

    /**
     * Starts the Engine.
     *
     * <p>Note, after the creation and before the call to start, the store might
     * be changed.
     */
    void start() throws EngineException;

    void create(Create create) throws EngineException;

    void index(Index index) throws EngineException;

    void delete(Delete delete) throws EngineException;

    void delete(DeleteByQuery delete) throws EngineException;

    Searcher searcher() throws EngineException;

    /**
     * Refreshes the engine for new search operations to reflect the latest
     * changes. Pass <tt>true</tt> if the refresh operation should include
     * all the operations performed up to this call.
     */
    void refresh(Refresh refresh) throws EngineException;

    /**
     * Flushes the state of the engine, clearing memory.
     */
    void flush(Flush flush) throws EngineException, FlushNotAllowedEngineException;

    void optimize(Optimize optimize) throws EngineException;

    <T> T snapshot(SnapshotHandler<T> snapshotHandler) throws EngineException;

    void recover(RecoveryHandler recoveryHandler) throws EngineException;

    /**
     * Returns the estimated flushable memory size. Returns <tt>null</tt> if not available.
     */
    ByteSizeValue estimateFlushableMemorySize();

    /**
     * Recovery allow to start the recovery process. It is built of three phases.
     *
     * <p>The first phase allows to take a snapshot of the master index. Once this
     * is taken, no commit operations are effectively allowed on the index until the recovery
     * phases are through.
     *
     * <p>The seconds phase takes a snapshot of the current transaction log.
     *
     * <p>The last phase returns the remaining transaction log. During this phase, no dirty
     * operations are allowed on the index.
     */
    static interface RecoveryHandler {

        void phase1(SnapshotIndexCommit snapshot) throws ElasticSearchException;

        void phase2(Translog.Snapshot snapshot) throws ElasticSearchException;

        void phase3(Translog.Snapshot snapshot) throws ElasticSearchException;
    }

    /**
     */
    static interface SnapshotHandler<T> {

        T snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot) throws EngineException;
    }

    static interface Searcher extends Releasable {

        IndexReader reader();

        IndexSearcher searcher();
    }

    static class Refresh {

        private final boolean waitForOperations;

        public Refresh(boolean waitForOperations) {
            this.waitForOperations = waitForOperations;
        }

        public boolean waitForOperations() {
            return waitForOperations;
        }

        @Override public String toString() {
            return "waitForOperations[" + waitForOperations + "]";
        }
    }

    static class Flush {

        private boolean full = false;
        private boolean refresh = false;

        /**
         * Should a refresh be performed after flushing. Defaults to <tt>false</tt>.
         */
        public boolean refresh() {
            return this.refresh;
        }

        /**
         * Should a refresh be performed after flushing. Defaults to <tt>false</tt>.
         */
        public Flush refresh(boolean refresh) {
            this.refresh = refresh;
            return this;
        }

        /**
         * Should a "full" flush be issued, basically cleaning as much memory as possible.
         */
        public boolean full() {
            return this.full;
        }

        /**
         * Should a "full" flush be issued, basically cleaning as much memory as possible.
         */
        public Flush full(boolean full) {
            this.full = full;
            return this;
        }

        @Override public String toString() {
            return "full[" + full + "], refresh[" + refresh + "]";
        }
    }

    static class Optimize {
        private boolean waitForMerge = true;
        private int maxNumSegments = -1;
        private boolean onlyExpungeDeletes = false;
        private boolean flush = false;
        private boolean refresh = false;

        public Optimize() {
        }

        public boolean waitForMerge() {
            return waitForMerge;
        }

        public Optimize waitForMerge(boolean waitForMerge) {
            this.waitForMerge = waitForMerge;
            return this;
        }

        public int maxNumSegments() {
            return maxNumSegments;
        }

        public Optimize maxNumSegments(int maxNumSegments) {
            this.maxNumSegments = maxNumSegments;
            return this;
        }

        public boolean onlyExpungeDeletes() {
            return onlyExpungeDeletes;
        }

        public Optimize onlyExpungeDeletes(boolean onlyExpungeDeletes) {
            this.onlyExpungeDeletes = onlyExpungeDeletes;
            return this;
        }

        public boolean flush() {
            return flush;
        }

        public Optimize flush(boolean flush) {
            this.flush = flush;
            return this;
        }

        public boolean refresh() {
            return refresh;
        }

        public Optimize refresh(boolean refresh) {
            this.refresh = refresh;
            return this;
        }

        @Override public String toString() {
            return "waitForMerge[" + waitForMerge + "], maxNumSegments[" + maxNumSegments + "], onlyExpungeDeletes[" + onlyExpungeDeletes + "], flush[" + flush + "], refresh[" + refresh + "]";
        }
    }

    static class Create {
        private final Document document;
        private final Analyzer analyzer;
        private final String type;
        private final String id;
        private final byte[] source;

        public Create(Document document, Analyzer analyzer, String type, String id, byte[] source) {
            this.document = document;
            this.analyzer = analyzer;
            this.type = type;
            this.id = id;
            this.source = source;
        }

        public String type() {
            return this.type;
        }

        public String id() {
            return this.id;
        }

        public Document doc() {
            return this.document;
        }

        public Analyzer analyzer() {
            return this.analyzer;
        }

        public byte[] source() {
            return this.source;
        }
    }

    static class Index {
        private final Term uid;
        private final Document document;
        private final Analyzer analyzer;
        private final String type;
        private final String id;
        private final byte[] source;

        public Index(Term uid, Document document, Analyzer analyzer, String type, String id, byte[] source) {
            this.uid = uid;
            this.document = document;
            this.analyzer = analyzer;
            this.type = type;
            this.id = id;
            this.source = source;
        }

        public Term uid() {
            return this.uid;
        }

        public Document doc() {
            return this.document;
        }

        public Analyzer analyzer() {
            return this.analyzer;
        }

        public String id() {
            return this.id;
        }

        public String type() {
            return this.type;
        }

        public byte[] source() {
            return this.source;
        }
    }

    static class Delete {
        private final Term uid;

        public Delete(Term uid) {
            this.uid = uid;
        }

        public Term uid() {
            return this.uid;
        }
    }

    static class DeleteByQuery {
        private final Query query;
        private final String queryParserName;
        private final byte[] source;
        private final String[] types;

        public DeleteByQuery(Query query, byte[] source, @Nullable String queryParserName, String... types) {
            this.query = query;
            this.source = source;
            this.queryParserName = queryParserName;
            this.types = types;
        }

        public String queryParserName() {
            return this.queryParserName;
        }

        public Query query() {
            return this.query;
        }

        public byte[] source() {
            return this.source;
        }

        public String[] types() {
            return this.types;
        }
    }
}
