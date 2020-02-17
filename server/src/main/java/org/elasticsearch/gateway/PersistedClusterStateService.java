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
package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.index.Index;

import java.io.Closeable;
import java.io.FilterOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntPredicate;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Stores cluster metadata in a bare Lucene index (per data path) split across a number of documents. This is used by master-eligible nodes
 * to record the last-accepted cluster state during publication. The metadata is written incrementally where possible, leaving alone any
 * documents that have not changed. The index has the following fields:
 *
 * +------------------------------+-----------------------------+----------------------------------------------+
 * | "type" (string field)        | "index_uuid" (string field) | "data" (stored binary field in SMILE format) |
 * +------------------------------+-----------------------------+----------------------------------------------+
 * | GLOBAL_TYPE_NAME == "global" | (omitted)                   | Global metadata                              |
 * | INDEX_TYPE_NAME  == "index"  | Index UUID                  | Index metadata                               |
 * +------------------------------+-----------------------------+----------------------------------------------+
 *
 * Additionally each commit has the following user data:
 *
 * +---------------------------+-------------------------+-------------------------------------------------------------------------------+
 * |        Key symbol         |       Key literal       |                                     Value                                     |
 * +---------------------------+-------------------------+-------------------------------------------------------------------------------+
 * | CURRENT_TERM_KEY          | "current_term"          | Node's "current" term (≥ last-accepted term and the terms of all sent joins)  |
 * | LAST_ACCEPTED_VERSION_KEY | "last_accepted_version" | The cluster state version corresponding with the persisted metadata           |
 * | NODE_ID_KEY               | "node_id"               | The (persistent) ID of the node that wrote this metadata                      |
 * | NODE_VERSION_KEY          | "node_version"          | The (ID of the) version of the node that wrote this metadata                  |
 * +---------------------------+-------------------------+-------------------------------------------------------------------------------+
 *
 * (the last-accepted term is recorded in MetaData → CoordinationMetaData so does not need repeating here)
 */
public class PersistedClusterStateService {
    private static final Logger logger = LogManager.getLogger(PersistedClusterStateService.class);
    private static final String CURRENT_TERM_KEY = "current_term";
    private static final String LAST_ACCEPTED_VERSION_KEY = "last_accepted_version";
    private static final String NODE_ID_KEY = "node_id";
    private static final String NODE_VERSION_KEY = "node_version";
    private static final String TYPE_FIELD_NAME = "type";
    private static final String DATA_FIELD_NAME = "data";
    private static final String GLOBAL_TYPE_NAME = "global";
    private static final String INDEX_TYPE_NAME = "index";
    private static final String INDEX_UUID_FIELD_NAME = "index_uuid";
    private static final int COMMIT_DATA_SIZE = 4;

    public static final String METADATA_DIRECTORY_NAME = MetaDataStateFormat.STATE_DIR_NAME;

    public static final Setting<TimeValue> SLOW_WRITE_LOGGING_THRESHOLD = Setting.timeSetting("gateway.slow_write_logging_threshold",
        TimeValue.timeValueSeconds(10), TimeValue.ZERO, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private final Path[] dataPaths;
    private final String nodeId;
    private final NamedXContentRegistry namedXContentRegistry;
    private final BigArrays bigArrays;
    private final boolean preserveUnknownCustoms;
    private final LongSupplier relativeTimeMillisSupplier;

    private volatile TimeValue slowWriteLoggingThreshold;

    public PersistedClusterStateService(NodeEnvironment nodeEnvironment, NamedXContentRegistry namedXContentRegistry, BigArrays bigArrays,
                                        ClusterSettings clusterSettings, LongSupplier relativeTimeMillisSupplier) {
        this(nodeEnvironment.nodeDataPaths(), nodeEnvironment.nodeId(), namedXContentRegistry, bigArrays, clusterSettings,
            relativeTimeMillisSupplier, false);
    }

    public PersistedClusterStateService(Path[] dataPaths, String nodeId, NamedXContentRegistry namedXContentRegistry, BigArrays bigArrays,
                                        ClusterSettings clusterSettings, LongSupplier relativeTimeMillisSupplier,
                                        boolean preserveUnknownCustoms) {
        this.dataPaths = dataPaths;
        this.nodeId = nodeId;
        this.namedXContentRegistry = namedXContentRegistry;
        this.bigArrays = bigArrays;
        this.relativeTimeMillisSupplier = relativeTimeMillisSupplier;
        this.preserveUnknownCustoms = preserveUnknownCustoms;
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    public String getNodeId() {
        return nodeId;
    }

    /**
     * Creates a new disk-based writer for cluster states
     */
    public Writer createWriter() throws IOException {
        final List<MetaDataIndexWriter> metaDataIndexWriters = new ArrayList<>();
        final List<Closeable> closeables = new ArrayList<>();
        boolean success = false;
        try {
            for (final Path path : dataPaths) {
                final Directory directory = createDirectory(path.resolve(METADATA_DIRECTORY_NAME));
                closeables.add(directory);

                final IndexWriter indexWriter = createIndexWriter(directory, false);
                closeables.add(indexWriter);
                metaDataIndexWriters.add(new MetaDataIndexWriter(directory, indexWriter));
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(closeables);
            }
        }
        return new Writer(metaDataIndexWriters, nodeId, bigArrays, relativeTimeMillisSupplier, () -> slowWriteLoggingThreshold);
    }

    private static IndexWriter createIndexWriter(Directory directory, boolean openExisting) throws IOException {
        final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        // start empty since we re-write the whole cluster state to ensure it is all using the same format version
        indexWriterConfig.setOpenMode(openExisting ? IndexWriterConfig.OpenMode.APPEND : IndexWriterConfig.OpenMode.CREATE);
        // only commit when specifically instructed, we must not write any intermediate states
        indexWriterConfig.setCommitOnClose(false);
        // most of the data goes into stored fields which are not buffered, so we only really need a tiny buffer
        indexWriterConfig.setRAMBufferSizeMB(1.0);
        // merge on the write thread (e.g. while flushing)
        indexWriterConfig.setMergeScheduler(new SerialMergeScheduler());

        return new IndexWriter(directory, indexWriterConfig);
    }

    /**
     * Remove all persisted cluster states from the given data paths, for use in tests. Should only be called when there is no open
     * {@link Writer} on these paths.
     */
    public static void deleteAll(Path[] dataPaths) throws IOException {
        for (Path dataPath : dataPaths) {
            Lucene.cleanLuceneIndex(new SimpleFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)));
        }
    }

    // exposed for tests
    Directory createDirectory(Path path) throws IOException {
        // it is possible to disable the use of MMapDirectory for indices, and it may be surprising to users that have done so if we still
        // use a MMapDirectory here, which might happen with FSDirectory.open(path). Concurrency is of no concern here so a
        // SimpleFSDirectory is fine:
        return new SimpleFSDirectory(path);
    }

    public Path[] getDataPaths() {
        return dataPaths;
    }

    public static class OnDiskState {
        private static final OnDiskState NO_ON_DISK_STATE = new OnDiskState(null, null, 0L, 0L, MetaData.EMPTY_META_DATA);

        private final String nodeId;
        private final Path dataPath;
        public final long currentTerm;
        public final long lastAcceptedVersion;
        public final MetaData metaData;

        private OnDiskState(String nodeId, Path dataPath, long currentTerm, long lastAcceptedVersion, MetaData metaData) {
            this.nodeId = nodeId;
            this.dataPath = dataPath;
            this.currentTerm = currentTerm;
            this.lastAcceptedVersion = lastAcceptedVersion;
            this.metaData = metaData;
        }

        public boolean empty() {
            return this == NO_ON_DISK_STATE;
        }
    }

    /**
     * Returns the node metadata for the given data paths, and checks if the node ids are unique
     * @param dataPaths the data paths to scan
     */
    @Nullable
    public static NodeMetaData nodeMetaData(Path... dataPaths) throws IOException {
        String nodeId = null;
        Version version = null;
        for (final Path dataPath : dataPaths) {
            final Path indexPath = dataPath.resolve(METADATA_DIRECTORY_NAME);
            if (Files.exists(indexPath)) {
                try (DirectoryReader reader = DirectoryReader.open(new SimpleFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)))) {
                    final Map<String, String> userData = reader.getIndexCommit().getUserData();
                    assert userData.get(NODE_VERSION_KEY) != null;

                    final String thisNodeId = userData.get(NODE_ID_KEY);
                    assert thisNodeId != null;
                    if (nodeId != null && nodeId.equals(thisNodeId) == false) {
                        throw new IllegalStateException("unexpected node ID in metadata, found [" + thisNodeId +
                            "] in [" + dataPath + "] but expected [" + nodeId + "]");
                    } else if (nodeId == null) {
                        nodeId = thisNodeId;
                        version = Version.fromId(Integer.parseInt(userData.get(NODE_VERSION_KEY)));
                    }
                } catch (IndexNotFoundException e) {
                    logger.debug(new ParameterizedMessage("no on-disk state at {}", indexPath), e);
                }
            }
        }
        if (nodeId == null) {
            return null;
        }
        return new NodeMetaData(nodeId, version);
    }

    /**
     * Overrides the version field for the metadata in the given data path
     */
    public static void overrideVersion(Version newVersion, Path... dataPaths) throws IOException {
        for (final Path dataPath : dataPaths) {
            final Path indexPath = dataPath.resolve(METADATA_DIRECTORY_NAME);
            if (Files.exists(indexPath)) {
                try (DirectoryReader reader = DirectoryReader.open(new SimpleFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)))) {
                    final Map<String, String> userData = reader.getIndexCommit().getUserData();
                    assert userData.get(NODE_VERSION_KEY) != null;

                    try (IndexWriter indexWriter =
                             createIndexWriter(new SimpleFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)), true)) {
                        final Map<String, String> commitData = new HashMap<>(userData);
                        commitData.put(NODE_VERSION_KEY, Integer.toString(newVersion.id));
                        indexWriter.setLiveCommitData(commitData.entrySet());
                        indexWriter.commit();
                    }
                } catch (IndexNotFoundException e) {
                    logger.debug(new ParameterizedMessage("no on-disk state at {}", indexPath), e);
                }
            }
        }
    }

    /**
     * Loads the best available on-disk cluster state. Returns {@link OnDiskState#NO_ON_DISK_STATE} if no such state was found.
     */
    public OnDiskState loadBestOnDiskState() throws IOException {
        String committedClusterUuid = null;
        Path committedClusterUuidPath = null;
        OnDiskState bestOnDiskState = OnDiskState.NO_ON_DISK_STATE;
        OnDiskState maxCurrentTermOnDiskState = bestOnDiskState;

        // We use a write-all-read-one strategy: metadata is written to every data path when accepting it, which means it is mostly
        // sufficient to read _any_ copy. "Mostly" sufficient because the user can change the set of data paths when restarting, and may
        // add a data path containing a stale copy of the metadata. We deal with this by using the freshest copy we can find.
        for (final Path dataPath : dataPaths) {
            final Path indexPath = dataPath.resolve(METADATA_DIRECTORY_NAME);
            if (Files.exists(indexPath)) {
                try (Directory directory = createDirectory(indexPath);
                     DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                    final OnDiskState onDiskState = loadOnDiskState(dataPath, directoryReader);

                    if (nodeId.equals(onDiskState.nodeId) == false) {
                        throw new IllegalStateException("unexpected node ID in metadata, found [" + onDiskState.nodeId +
                            "] in [" + dataPath + "] but expected [" + nodeId + "]");
                    }

                    if (onDiskState.metaData.clusterUUIDCommitted()) {
                        if (committedClusterUuid == null) {
                            committedClusterUuid = onDiskState.metaData.clusterUUID();
                            committedClusterUuidPath = dataPath;
                        } else if (committedClusterUuid.equals(onDiskState.metaData.clusterUUID()) == false) {
                            throw new IllegalStateException("mismatched cluster UUIDs in metadata, found [" + committedClusterUuid +
                                "] in [" + committedClusterUuidPath + "] and [" + onDiskState.metaData.clusterUUID() + "] in ["
                                + dataPath + "]");
                        }
                    }

                    if (maxCurrentTermOnDiskState.empty() || maxCurrentTermOnDiskState.currentTerm < onDiskState.currentTerm) {
                        maxCurrentTermOnDiskState = onDiskState;
                    }

                    long acceptedTerm = onDiskState.metaData.coordinationMetaData().term();
                    long maxAcceptedTerm = bestOnDiskState.metaData.coordinationMetaData().term();
                    if (bestOnDiskState.empty()
                        || acceptedTerm > maxAcceptedTerm
                        || (acceptedTerm == maxAcceptedTerm
                            && (onDiskState.lastAcceptedVersion > bestOnDiskState.lastAcceptedVersion
                                || (onDiskState.lastAcceptedVersion == bestOnDiskState.lastAcceptedVersion)
                                    && onDiskState.currentTerm > bestOnDiskState.currentTerm))) {
                        bestOnDiskState = onDiskState;
                    }
                } catch (IndexNotFoundException e) {
                    logger.debug(new ParameterizedMessage("no on-disk state at {}", indexPath), e);
                }
            }
        }

        if (bestOnDiskState.currentTerm != maxCurrentTermOnDiskState.currentTerm) {
            throw new IllegalStateException("inconsistent terms found: best state is from [" + bestOnDiskState.dataPath +
                "] in term [" + bestOnDiskState.currentTerm + "] but there is a stale state in [" + maxCurrentTermOnDiskState.dataPath +
                "] with greater term [" + maxCurrentTermOnDiskState.currentTerm + "]");
        }

        return bestOnDiskState;
    }

    private OnDiskState loadOnDiskState(Path dataPath, DirectoryReader reader) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);

        final SetOnce<MetaData.Builder> builderReference = new SetOnce<>();
        consumeFromType(searcher, GLOBAL_TYPE_NAME, bytes ->
        {
            final MetaData metaData = MetaData.Builder.fromXContent(XContentFactory.xContent(XContentType.SMILE)
                .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, bytes.bytes, bytes.offset, bytes.length),
                preserveUnknownCustoms);
            logger.trace("found global metadata with last-accepted term [{}]", metaData.coordinationMetaData().term());
            if (builderReference.get() != null) {
                throw new IllegalStateException("duplicate global metadata found in [" + dataPath + "]");
            }
            builderReference.set(MetaData.builder(metaData));
        });

        final MetaData.Builder builder = builderReference.get();
        if (builder == null) {
            throw new IllegalStateException("no global metadata found in [" + dataPath + "]");
        }

        logger.trace("got global metadata, now reading index metadata");

        final Set<String> indexUUIDs = new HashSet<>();
        consumeFromType(searcher, INDEX_TYPE_NAME, bytes ->
        {
            final IndexMetaData indexMetaData = IndexMetaData.fromXContent(XContentFactory.xContent(XContentType.SMILE)
                .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, bytes.bytes, bytes.offset, bytes.length));
            logger.trace("found index metadata for {}", indexMetaData.getIndex());
            if (indexUUIDs.add(indexMetaData.getIndexUUID()) == false) {
                throw new IllegalStateException("duplicate metadata found for " + indexMetaData.getIndex() + " in [" + dataPath + "]");
            }
            builder.put(indexMetaData, false);
        });

        final Map<String, String> userData = reader.getIndexCommit().getUserData();
        logger.trace("loaded metadata [{}] from [{}]", userData, reader.directory());
        assert userData.size() == COMMIT_DATA_SIZE : userData;
        assert userData.get(CURRENT_TERM_KEY) != null;
        assert userData.get(LAST_ACCEPTED_VERSION_KEY) != null;
        assert userData.get(NODE_ID_KEY) != null;
        assert userData.get(NODE_VERSION_KEY) != null;
        return new OnDiskState(userData.get(NODE_ID_KEY), dataPath, Long.parseLong(userData.get(CURRENT_TERM_KEY)),
            Long.parseLong(userData.get(LAST_ACCEPTED_VERSION_KEY)), builder.build());
    }

    private static void consumeFromType(IndexSearcher indexSearcher, String type,
                                        CheckedConsumer<BytesRef, IOException> bytesRefConsumer) throws IOException {

        final Query query = new TermQuery(new Term(TYPE_FIELD_NAME, type));
        final Weight weight = indexSearcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
        logger.trace("running query [{}]", query);

        for (LeafReaderContext leafReaderContext : indexSearcher.getIndexReader().leaves()) {
            logger.trace("new leafReaderContext: {}", leafReaderContext);
            final Scorer scorer = weight.scorer(leafReaderContext);
            if (scorer != null) {
                final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
                final IntPredicate isLiveDoc = liveDocs == null ? i -> true : liveDocs::get;
                final DocIdSetIterator docIdSetIterator = scorer.iterator();
                while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    if (isLiveDoc.test(docIdSetIterator.docID())) {
                        logger.trace("processing doc {}", docIdSetIterator.docID());
                        bytesRefConsumer.accept(
                            leafReaderContext.reader().document(docIdSetIterator.docID()).getBinaryValue(DATA_FIELD_NAME));
                    }
                }
            }
        }
    }

    private static final ToXContent.Params FORMAT_PARAMS;

    static {
        Map<String, String> params = new HashMap<>(2);
        params.put("binary", "true");
        params.put(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new ToXContent.MapParams(params);
    }

    /**
     * A {@link Document} with a stored field containing serialized metadata written to a {@link ReleasableBytesStreamOutput} which must be
     * released when no longer needed.
     */
    private static class ReleasableDocument implements Releasable {
        private final Document document;
        private final Releasable releasable;

        ReleasableDocument(Document document, Releasable releasable) {
            this.document = document;
            this.releasable = releasable;
        }

        Document getDocument() {
            return document;
        }

        @Override
        public void close() {
            releasable.close();
        }
    }

    /**
     * Encapsulates a single {@link IndexWriter} with its {@link Directory} for ease of closing, and a {@link Logger}. There is one of these
     * for each data path.
     */
    private static class MetaDataIndexWriter implements Closeable {

        private final Logger logger;
        private final Directory directory;
        private final IndexWriter indexWriter;

        MetaDataIndexWriter(Directory directory, IndexWriter indexWriter) {
            this.directory = directory;
            this.indexWriter = indexWriter;
            this.logger = Loggers.getLogger(MetaDataIndexWriter.class, directory.toString());
        }

        void deleteAll() throws IOException {
            this.logger.trace("clearing existing metadata");
            this.indexWriter.deleteAll();
        }

        void updateIndexMetaDataDocument(Document indexMetaDataDocument, Index index) throws IOException {
            this.logger.trace("updating metadata for [{}]", index);
            indexWriter.updateDocument(new Term(INDEX_UUID_FIELD_NAME, index.getUUID()), indexMetaDataDocument);
        }

        void updateGlobalMetaData(Document globalMetaDataDocument) throws IOException {
            this.logger.trace("updating global metadata doc");
            indexWriter.updateDocument(new Term(TYPE_FIELD_NAME, GLOBAL_TYPE_NAME), globalMetaDataDocument);
        }

        void deleteIndexMetaData(String indexUUID) throws IOException {
            this.logger.trace("removing metadata for [{}]", indexUUID);
            indexWriter.deleteDocuments(new Term(INDEX_UUID_FIELD_NAME, indexUUID));
        }

        void flush() throws IOException {
            this.logger.trace("flushing");
            this.indexWriter.flush();
        }

        void prepareCommit(String nodeId, long currentTerm, long lastAcceptedVersion) throws IOException {
            final Map<String, String> commitData = new HashMap<>(COMMIT_DATA_SIZE);
            commitData.put(CURRENT_TERM_KEY, Long.toString(currentTerm));
            commitData.put(LAST_ACCEPTED_VERSION_KEY, Long.toString(lastAcceptedVersion));
            commitData.put(NODE_VERSION_KEY, Integer.toString(Version.CURRENT.id));
            commitData.put(NODE_ID_KEY, nodeId);
            indexWriter.setLiveCommitData(commitData.entrySet());
            indexWriter.prepareCommit();
        }

        void commit() throws IOException {
            indexWriter.commit();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(indexWriter, directory);
        }
    }

    public static class Writer implements Closeable {

        private final List<MetaDataIndexWriter> metaDataIndexWriters;
        private final String nodeId;
        private final BigArrays bigArrays;
        private final LongSupplier relativeTimeMillisSupplier;
        private final Supplier<TimeValue> slowWriteLoggingThresholdSupplier;

        boolean fullStateWritten = false;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Writer(List<MetaDataIndexWriter> metaDataIndexWriters, String nodeId, BigArrays bigArrays,
                       LongSupplier relativeTimeMillisSupplier, Supplier<TimeValue> slowWriteLoggingThresholdSupplier) {
            this.metaDataIndexWriters = metaDataIndexWriters;
            this.nodeId = nodeId;
            this.bigArrays = bigArrays;
            this.relativeTimeMillisSupplier = relativeTimeMillisSupplier;
            this.slowWriteLoggingThresholdSupplier = slowWriteLoggingThresholdSupplier;
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new AlreadyClosedException("cluster state writer is closed already");
            }
        }

        public boolean isOpen() {
            return closed.get() == false;
        }

        private void closeIfAnyIndexWriterHasTragedyOrIsClosed() {
            if (metaDataIndexWriters.stream().map(writer -> writer.indexWriter)
                .anyMatch(iw -> iw.getTragicException() != null || iw.isOpen() == false)) {
                try {
                    close();
                } catch (Exception e) {
                    logger.warn("failed on closing cluster state writer", e);
                }
            }
        }

        /**
         * Overrides and commits the given current term and cluster state
         */
        public void writeFullStateAndCommit(long currentTerm, ClusterState clusterState) throws IOException {
            ensureOpen();
            try {
                final long startTimeMillis = relativeTimeMillisSupplier.getAsLong();
                final WriterStats stats = overwriteMetaData(clusterState.metaData());
                commit(currentTerm, clusterState.version());
                fullStateWritten = true;
                final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
                final TimeValue finalSlowWriteLoggingThreshold = slowWriteLoggingThresholdSupplier.get();
                if (durationMillis >= finalSlowWriteLoggingThreshold.getMillis()) {
                    logger.warn("writing cluster state took [{}ms] which is above the warn threshold of [{}]; " +
                            "wrote full state with [{}] indices",
                        durationMillis, finalSlowWriteLoggingThreshold, stats.numIndicesUpdated);
                } else {
                    logger.debug("writing cluster state took [{}ms]; " +
                            "wrote full state with [{}] indices",
                        durationMillis, stats.numIndicesUpdated);
                }
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
        }

        /**
         * Updates and commits the given cluster state update
         */
        void writeIncrementalStateAndCommit(long currentTerm, ClusterState previousClusterState,
                                            ClusterState clusterState) throws IOException {
            ensureOpen();
            assert fullStateWritten : "Need to write full state first before doing incremental writes";
            try {
                final long startTimeMillis = relativeTimeMillisSupplier.getAsLong();
                final WriterStats stats = updateMetaData(previousClusterState.metaData(), clusterState.metaData());
                commit(currentTerm, clusterState.version());
                final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
                final TimeValue finalSlowWriteLoggingThreshold = slowWriteLoggingThresholdSupplier.get();
                if (durationMillis >= finalSlowWriteLoggingThreshold.getMillis()) {
                    logger.warn("writing cluster state took [{}ms] which is above the warn threshold of [{}]; " +
                            "wrote global metadata [{}] and metadata for [{}] indices and skipped [{}] unchanged indices",
                        durationMillis, finalSlowWriteLoggingThreshold, stats.globalMetaUpdated, stats.numIndicesUpdated,
                        stats.numIndicesUnchanged);
                } else {
                    logger.debug("writing cluster state took [{}ms]; " +
                            "wrote global metadata [{}] and metadata for [{}] indices and skipped [{}] unchanged indices",
                        durationMillis, stats.globalMetaUpdated, stats.numIndicesUpdated, stats.numIndicesUnchanged);
                }
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
        }

        /**
         * Update the persisted metadata to match the given cluster state by removing any stale or unnecessary documents and adding any
         * updated documents.
         */
        private WriterStats updateMetaData(MetaData previouslyWrittenMetaData, MetaData metaData) throws IOException {
            assert previouslyWrittenMetaData.coordinationMetaData().term() == metaData.coordinationMetaData().term();
            logger.trace("currentTerm [{}] matches previous currentTerm, writing changes only",
                metaData.coordinationMetaData().term());

            final boolean updateGlobalMeta = MetaData.isGlobalStateEquals(previouslyWrittenMetaData, metaData) == false;
            if (updateGlobalMeta) {
                try (ReleasableDocument globalMetaDataDocument = makeGlobalMetaDataDocument(metaData)) {
                    for (MetaDataIndexWriter metaDataIndexWriter : metaDataIndexWriters) {
                        metaDataIndexWriter.updateGlobalMetaData(globalMetaDataDocument.getDocument());
                    }
                }
            }

            final Map<String, Long> indexMetaDataVersionByUUID = new HashMap<>(previouslyWrittenMetaData.indices().size());
            for (ObjectCursor<IndexMetaData> cursor : previouslyWrittenMetaData.indices().values()) {
                final IndexMetaData indexMetaData = cursor.value;
                final Long previousValue = indexMetaDataVersionByUUID.putIfAbsent(indexMetaData.getIndexUUID(), indexMetaData.getVersion());
                assert previousValue == null : indexMetaData.getIndexUUID() + " already mapped to " + previousValue;
            }

            int numIndicesUpdated = 0;
            int numIndicesUnchanged = 0;
            for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
                final IndexMetaData indexMetaData = cursor.value;
                final Long previousVersion = indexMetaDataVersionByUUID.get(indexMetaData.getIndexUUID());
                if (previousVersion == null || indexMetaData.getVersion() != previousVersion) {
                    logger.trace("updating metadata for [{}], changing version from [{}] to [{}]",
                        indexMetaData.getIndex(), previousVersion, indexMetaData.getVersion());
                    numIndicesUpdated++;
                    try (ReleasableDocument indexMetaDataDocument = makeIndexMetaDataDocument(indexMetaData)) {
                        for (MetaDataIndexWriter metaDataIndexWriter : metaDataIndexWriters) {
                            metaDataIndexWriter.updateIndexMetaDataDocument(indexMetaDataDocument.getDocument(), indexMetaData.getIndex());
                        }
                    }
                } else {
                    numIndicesUnchanged++;
                    logger.trace("no action required for [{}]", indexMetaData.getIndex());
                }
                indexMetaDataVersionByUUID.remove(indexMetaData.getIndexUUID());
            }

            for (String removedIndexUUID : indexMetaDataVersionByUUID.keySet()) {
                for (MetaDataIndexWriter metaDataIndexWriter : metaDataIndexWriters) {
                    metaDataIndexWriter.deleteIndexMetaData(removedIndexUUID);
                }
            }

            // Flush, to try and expose a failure (e.g. out of disk space) before committing, because we can handle a failure here more
            // gracefully than one that occurs during the commit process.
            for (MetaDataIndexWriter metaDataIndexWriter : metaDataIndexWriters) {
                metaDataIndexWriter.flush();
            }

            return new WriterStats(updateGlobalMeta, numIndicesUpdated, numIndicesUnchanged);
        }

        /**
         * Update the persisted metadata to match the given cluster state by removing all existing documents and then adding new documents.
         */
        private WriterStats overwriteMetaData(MetaData metaData) throws IOException {
            for (MetaDataIndexWriter metaDataIndexWriter : metaDataIndexWriters) {
                metaDataIndexWriter.deleteAll();
            }
            return addMetaData(metaData);
        }

        /**
         * Add documents for the metadata of the given cluster state, assuming that there are currently no documents.
         */
        private WriterStats addMetaData(MetaData metaData) throws IOException {
            try (ReleasableDocument globalMetaDataDocument = makeGlobalMetaDataDocument(metaData)) {
                for (MetaDataIndexWriter metaDataIndexWriter : metaDataIndexWriters) {
                    metaDataIndexWriter.updateGlobalMetaData(globalMetaDataDocument.getDocument());
                }
            }

            for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
                final IndexMetaData indexMetaData = cursor.value;
                try (ReleasableDocument indexMetaDataDocument = makeIndexMetaDataDocument(indexMetaData)) {
                    for (MetaDataIndexWriter metaDataIndexWriter : metaDataIndexWriters) {
                        metaDataIndexWriter.updateIndexMetaDataDocument(indexMetaDataDocument.getDocument(), indexMetaData.getIndex());
                    }
                }
            }

            // Flush, to try and expose a failure (e.g. out of disk space) before committing, because we can handle a failure here more
            // gracefully than one that occurs during the commit process.
            for (MetaDataIndexWriter metaDataIndexWriter : metaDataIndexWriters) {
                metaDataIndexWriter.flush();
            }

            return new WriterStats(true, metaData.indices().size(), 0);
        }

        public void commit(long currentTerm, long lastAcceptedVersion) throws IOException {
            ensureOpen();
            try {
                for (MetaDataIndexWriter metaDataIndexWriter : metaDataIndexWriters) {
                    metaDataIndexWriter.prepareCommit(nodeId, currentTerm, lastAcceptedVersion);
                }
            } catch (Exception e) {
                try {
                    close();
                } catch (Exception e2) {
                    logger.warn("failed on closing cluster state writer", e2);
                    e.addSuppressed(e2);
                }
                throw e;
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
            try {
                for (MetaDataIndexWriter metaDataIndexWriter : metaDataIndexWriters) {
                    metaDataIndexWriter.commit();
                }
            } catch (IOException e) {
                // The commit() call has similar semantics to a fsync(): although it's atomic, if it fails then we've no idea whether the
                // data on disk is now the old version or the new version, and this is a disaster. It's safest to fail the whole node and
                // retry from the beginning.
                try {
                    close();
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                }
                throw new IOError(e);
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
        }

        @Override
        public void close() throws IOException {
            logger.trace("closing PersistedClusterStateService.Writer");
            if (closed.compareAndSet(false, true)) {
                IOUtils.close(metaDataIndexWriters);
            }
        }

        static class WriterStats {
            final boolean globalMetaUpdated;
            final long numIndicesUpdated;
            final long numIndicesUnchanged;

            WriterStats(boolean globalMetaUpdated, long numIndicesUpdated, long numIndicesUnchanged) {
                this.globalMetaUpdated = globalMetaUpdated;
                this.numIndicesUpdated = numIndicesUpdated;
                this.numIndicesUnchanged = numIndicesUnchanged;
            }
        }

        private ReleasableDocument makeIndexMetaDataDocument(IndexMetaData indexMetaData) throws IOException {
            final ReleasableDocument indexMetaDataDocument = makeDocument(INDEX_TYPE_NAME, indexMetaData);
            boolean success = false;
            try {
                final String indexUUID = indexMetaData.getIndexUUID();
                assert indexUUID.equals(IndexMetaData.INDEX_UUID_NA_VALUE) == false;
                indexMetaDataDocument.getDocument().add(new StringField(INDEX_UUID_FIELD_NAME, indexUUID, Field.Store.NO));
                success = true;
                return indexMetaDataDocument;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(indexMetaDataDocument);
                }
            }
        }

        private ReleasableDocument makeGlobalMetaDataDocument(MetaData metaData) throws IOException {
            return makeDocument(GLOBAL_TYPE_NAME, metaData);
        }

        private ReleasableDocument makeDocument(String typeName, ToXContent metaData) throws IOException {
            final Document document = new Document();
            document.add(new StringField(TYPE_FIELD_NAME, typeName, Field.Store.NO));

            boolean success = false;
            final ReleasableBytesStreamOutput releasableBytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
            try {
                final FilterOutputStream outputStream = new FilterOutputStream(releasableBytesStreamOutput) {
                    @Override
                    public void close() {
                        // closing the XContentBuilder should not release the bytes yet
                    }
                };
                try (XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.SMILE, outputStream)) {
                    xContentBuilder.startObject();
                    metaData.toXContent(xContentBuilder, FORMAT_PARAMS);
                    xContentBuilder.endObject();
                }
                document.add(new StoredField(DATA_FIELD_NAME, releasableBytesStreamOutput.bytes().toBytesRef()));
                final ReleasableDocument releasableDocument = new ReleasableDocument(document, releasableBytesStreamOutput);
                success = true;
                return releasableDocument;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(releasableBytesStreamOutput);
                }
            }
        }
    }
}
