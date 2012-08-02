/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
import org.apache.lucene.index.ExtendedIndexSearcher;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.CloseableComponent;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.util.List;

/**
 *
 */
public interface Engine extends IndexShardComponent, CloseableComponent {

    static ByteSizeValue INACTIVE_SHARD_INDEXING_BUFFER = ByteSizeValue.parseBytesSizeValue("500kb");

    /**
     * The default suggested refresh interval, -1 to disable it.
     */
    TimeValue defaultRefreshInterval();

    void enableGcDeletes(boolean enableGcDeletes);

    void updateIndexingBufferSize(ByteSizeValue indexingBufferSize);

    void addFailedEngineListener(FailedEngineListener listener);

    /**
     * Starts the Engine.
     * <p/>
     * <p>Note, after the creation and before the call to start, the store might
     * be changed.
     */
    void start() throws EngineException;

    void create(Create create) throws EngineException;

    void index(Index index) throws EngineException;

    void delete(Delete delete) throws EngineException;

    void delete(DeleteByQuery delete) throws EngineException;

    GetResult get(Get get) throws EngineException;

    Searcher searcher() throws EngineException;

    List<Segment> segments();

    /**
     * Returns <tt>true</tt> if a refresh is really needed.
     */
    boolean refreshNeeded();

    /**
     * Returns <tt>true</tt> if a possible merge is really needed.
     */
    boolean possibleMergeNeeded();

    void maybeMerge() throws EngineException;

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

    static interface FailedEngineListener {
        void onFailedEngine(ShardId shardId, Throwable t);
    }

    /**
     * Recovery allow to start the recovery process. It is built of three phases.
     * <p/>
     * <p>The first phase allows to take a snapshot of the master index. Once this
     * is taken, no commit operations are effectively allowed on the index until the recovery
     * phases are through.
     * <p/>
     * <p>The seconds phase takes a snapshot of the current transaction log.
     * <p/>
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

        ExtendedIndexSearcher searcher();
    }

    static class SimpleSearcher implements Searcher {

        private final IndexSearcher searcher;

        public SimpleSearcher(IndexSearcher searcher) {
            this.searcher = searcher;
        }

        @Override
        public IndexReader reader() {
            return searcher.getIndexReader();
        }

        @Override
        public ExtendedIndexSearcher searcher() {
            return (ExtendedIndexSearcher) searcher;
        }

        @Override
        public boolean release() throws ElasticSearchException {
            // nothing to release here...
            return true;
        }
    }

    static class Refresh {

        private final boolean waitForOperations;

        private boolean force = false;

        public Refresh(boolean waitForOperations) {
            this.waitForOperations = waitForOperations;
        }

        public Refresh force(boolean force) {
            this.force = force;
            return this;
        }

        public boolean force() {
            return this.force;
        }

        public boolean waitForOperations() {
            return waitForOperations;
        }

        @Override
        public String toString() {
            return "waitForOperations[" + waitForOperations + "]";
        }
    }

    static class Flush {

        private boolean full = false;
        private boolean refresh = false;
        private boolean force = false;

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

        public boolean force() {
            return this.force;
        }

        public Flush force(boolean force) {
            this.force = force;
            return this;
        }

        @Override
        public String toString() {
            return "full[" + full + "], refresh[" + refresh + "], force[" + force + "]";
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

        @Override
        public String toString() {
            return "waitForMerge[" + waitForMerge + "], maxNumSegments[" + maxNumSegments + "], onlyExpungeDeletes[" + onlyExpungeDeletes + "], flush[" + flush + "], refresh[" + refresh + "]";
        }
    }

    static interface Operation {
        static enum Type {
            CREATE,
            INDEX,
            DELETE
        }

        static enum Origin {
            PRIMARY,
            REPLICA,
            RECOVERY
        }

        Type opType();

        Origin origin();
    }

    static interface IndexingOperation extends Operation {

        ParsedDocument parsedDoc();

        List<Document> docs();

        DocumentMapper docMapper();
    }

    static class Create implements IndexingOperation {
        private final DocumentMapper docMapper;
        private final Term uid;
        private final ParsedDocument doc;
        private long version;
        private VersionType versionType = VersionType.INTERNAL;
        private Origin origin = Origin.PRIMARY;

        private long startTime;
        private long endTime;

        public Create(DocumentMapper docMapper, Term uid, ParsedDocument doc) {
            this.docMapper = docMapper;
            this.uid = uid;
            this.doc = doc;
        }

        public DocumentMapper docMapper() {
            return this.docMapper;
        }

        @Override
        public Type opType() {
            return Type.CREATE;
        }

        public Create origin(Origin origin) {
            this.origin = origin;
            return this;
        }

        @Override
        public Origin origin() {
            return this.origin;
        }

        public ParsedDocument parsedDoc() {
            return this.doc;
        }

        public Term uid() {
            return this.uid;
        }

        public String type() {
            return this.doc.type();
        }

        public String id() {
            return this.doc.id();
        }

        public String routing() {
            return this.doc.routing();
        }

        public long timestamp() {
            return this.doc.timestamp();
        }

        public long ttl() {
            return this.doc.ttl();
        }

        public long version() {
            return this.version;
        }

        public Create version(long version) {
            this.version = version;
            return this;
        }

        public VersionType versionType() {
            return this.versionType;
        }

        public Create versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }

        public String parent() {
            return this.doc.parent();
        }

        public List<Document> docs() {
            return this.doc.docs();
        }

        public Analyzer analyzer() {
            return this.doc.analyzer();
        }

        public BytesReference source() {
            return this.doc.source();
        }

        public UidField uidField() {
            return (UidField) doc.rootDoc().getFieldable(UidFieldMapper.NAME);
        }


        public Create startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Returns operation start time in nanoseconds.
         */
        public long startTime() {
            return this.startTime;
        }

        public Create endTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        /**
         * Returns operation end time in nanoseconds.
         */
        public long endTime() {
            return this.endTime;
        }
    }

    static class Index implements IndexingOperation {
        private final DocumentMapper docMapper;
        private final Term uid;
        private final ParsedDocument doc;
        private long version;
        private VersionType versionType = VersionType.INTERNAL;
        private Origin origin = Origin.PRIMARY;

        private long startTime;
        private long endTime;

        public Index(DocumentMapper docMapper, Term uid, ParsedDocument doc) {
            this.docMapper = docMapper;
            this.uid = uid;
            this.doc = doc;
        }

        public DocumentMapper docMapper() {
            return this.docMapper;
        }

        @Override
        public Type opType() {
            return Type.INDEX;
        }

        public Index origin(Origin origin) {
            this.origin = origin;
            return this;
        }

        @Override
        public Origin origin() {
            return this.origin;
        }

        public Term uid() {
            return this.uid;
        }

        public ParsedDocument parsedDoc() {
            return this.doc;
        }

        public Index version(long version) {
            this.version = version;
            return this;
        }

        public long version() {
            return this.version;
        }

        public Index versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }

        public VersionType versionType() {
            return this.versionType;
        }

        public List<Document> docs() {
            return this.doc.docs();
        }

        public Analyzer analyzer() {
            return this.doc.analyzer();
        }

        public String id() {
            return this.doc.id();
        }

        public String type() {
            return this.doc.type();
        }

        public String routing() {
            return this.doc.routing();
        }

        public String parent() {
            return this.doc.parent();
        }

        public long timestamp() {
            return this.doc.timestamp();
        }

        public long ttl() {
            return this.doc.ttl();
        }

        public BytesReference source() {
            return this.doc.source();
        }

        public UidField uidField() {
            return (UidField) doc.rootDoc().getFieldable(UidFieldMapper.NAME);
        }

        public Index startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Returns operation start time in nanoseconds.
         */
        public long startTime() {
            return this.startTime;
        }

        public Index endTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        /**
         * Returns operation end time in nanoseconds.
         */
        public long endTime() {
            return this.endTime;
        }
    }

    static class Delete implements Operation {
        private final String type;
        private final String id;
        private final Term uid;
        private long version;
        private VersionType versionType = VersionType.INTERNAL;
        private Origin origin = Origin.PRIMARY;
        private boolean notFound;

        private long startTime;
        private long endTime;

        public Delete(String type, String id, Term uid) {
            this.type = type;
            this.id = id;
            this.uid = uid;
        }

        @Override
        public Type opType() {
            return Type.DELETE;
        }

        public Delete origin(Origin origin) {
            this.origin = origin;
            return this;
        }

        @Override
        public Origin origin() {
            return this.origin;
        }

        public String type() {
            return this.type;
        }

        public String id() {
            return this.id;
        }

        public Term uid() {
            return this.uid;
        }

        public Delete version(long version) {
            this.version = version;
            return this;
        }

        public long version() {
            return this.version;
        }

        public Delete versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }

        public VersionType versionType() {
            return this.versionType;
        }

        public boolean notFound() {
            return this.notFound;
        }

        public Delete notFound(boolean notFound) {
            this.notFound = notFound;
            return this;
        }


        public Delete startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Returns operation start time in nanoseconds.
         */
        public long startTime() {
            return this.startTime;
        }

        public Delete endTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        /**
         * Returns operation end time in nanoseconds.
         */
        public long endTime() {
            return this.endTime;
        }
    }

    static class DeleteByQuery {
        private final Query query;
        private final BytesReference source;
        private final String[] filteringAliases;
        private final Filter aliasFilter;
        private final String[] types;

        private long startTime;
        private long endTime;

        public DeleteByQuery(Query query, BytesReference source, @Nullable String[] filteringAliases, @Nullable Filter aliasFilter, String... types) {
            this.query = query;
            this.source = source;
            this.types = types;
            this.filteringAliases = filteringAliases;
            this.aliasFilter = aliasFilter;
        }

        public Query query() {
            return this.query;
        }

        public BytesReference source() {
            return this.source;
        }

        public String[] types() {
            return this.types;
        }

        public String[] filteringAliases() {
            return filteringAliases;
        }

        public Filter aliasFilter() {
            return aliasFilter;
        }

        public DeleteByQuery startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Returns operation start time in nanoseconds.
         */
        public long startTime() {
            return this.startTime;
        }

        public DeleteByQuery endTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        /**
         * Returns operation end time in nanoseconds.
         */
        public long endTime() {
            return this.endTime;
        }
    }


    static class Get {
        private final boolean realtime;
        private final Term uid;
        private boolean loadSource = true;

        public Get(boolean realtime, Term uid) {
            this.realtime = realtime;
            this.uid = uid;
        }

        public boolean realtime() {
            return this.realtime;
        }

        public Term uid() {
            return uid;
        }

        public boolean loadSource() {
            return this.loadSource;
        }

        public Get loadSource(boolean loadSource) {
            this.loadSource = loadSource;
            return this;
        }
    }

    static class GetResult {
        private final boolean exists;
        private final long version;
        private final Translog.Source source;
        private final UidField.DocIdAndVersion docIdAndVersion;
        private final Searcher searcher;

        public static final GetResult NOT_EXISTS = new GetResult(false, -1, null);

        public GetResult(boolean exists, long version, @Nullable Translog.Source source) {
            this.source = source;
            this.exists = exists;
            this.version = version;
            this.docIdAndVersion = null;
            this.searcher = null;
        }

        public GetResult(Searcher searcher, UidField.DocIdAndVersion docIdAndVersion) {
            this.exists = true;
            this.source = null;
            this.version = docIdAndVersion.version;
            this.docIdAndVersion = docIdAndVersion;
            this.searcher = searcher;
        }

        public boolean exists() {
            return exists;
        }

        public long version() {
            return this.version;
        }

        @Nullable
        public Translog.Source source() {
            return source;
        }

        public Searcher searcher() {
            return this.searcher;
        }

        public UidField.DocIdAndVersion docIdAndVersion() {
            return docIdAndVersion;
        }

        public void release() {
            if (searcher != null) {
                searcher.release();
            }
        }
    }

}
