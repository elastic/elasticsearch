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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.IntPredicate;

public class LucenePersistedStateFactory {
    private static final Logger logger = LogManager.getLogger(LucenePersistedStateFactory.class);
    private static final String CURRENT_TERM_KEY = "current_term";
    private static final String LAST_ACCEPTED_VERSION_KEY = "last_accepted_version";
    private static final String NODE_ID_KEY = "node_id";
    private static final String NODE_VERSION_KEY = "node_version";
    private static final String TYPE_FIELD_NAME = "type";
    private static final String DATA_FIELD_NAME = "data";
    private static final String GLOBAL_TYPE_NAME = "global";
    private static final String INDEX_TYPE_NAME = "index";
    private static final String INDEX_UUID_FIELD_NAME = "index_uuid";

    private final NodeEnvironment nodeEnvironment;
    private final NamedXContentRegistry namedXContentRegistry;

    public LucenePersistedStateFactory(NodeEnvironment nodeEnvironment, NamedXContentRegistry namedXContentRegistry) {
        this.nodeEnvironment = nodeEnvironment;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    CoordinationState.PersistedState loadPersistedState(BiFunction<Long, MetaData, ClusterState> clusterStateFromMetaData)
        throws IOException {

        final OnDiskState onDiskState = loadBestOnDiskState();

        final List<MetaDataIndex> metaDataIndices = new ArrayList<>();
        final List<Closeable> closeables = new ArrayList<>();
        boolean success = false;
        try {
            for (final Path path : nodeEnvironment.nodeDataPaths()) {
                final Directory directory = createDirectory(getMetaDataIndexPath(path, Version.CURRENT.major));
                closeables.add(directory);
                final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
                indexWriterConfig.setMergeScheduler(new SerialMergeScheduler());
                indexWriterConfig.setCommitOnClose(false);
                final IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
                closeables.add(indexWriter);
                metaDataIndices.add(new MetaDataIndex(directory, indexWriter));
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(closeables);
            }
        }

        final ClusterState clusterState = clusterStateFromMetaData.apply(onDiskState.lastAcceptedVersion, onDiskState.metaData);
        final LucenePersistedState lucenePersistedState
            = new LucenePersistedState(nodeEnvironment.nodeId(), metaDataIndices, onDiskState.currentTerm, clusterState);
        success = false;
        try {
            lucenePersistedState.persistInitialState();

            for (final Path path : nodeEnvironment.nodeDataPaths()) {
                assert Files.exists(getMetaDataIndexPath(path, Version.CURRENT.major - 1)) == false;
            }
            success = true;
            return lucenePersistedState;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(lucenePersistedState);
            }
        }
    }

    // exposed for tests
    Directory createDirectory(Path path) throws IOException {
        return new SimpleFSDirectory(path);
    }

    private static class OnDiskState {
        private final String nodeId;
        final long currentTerm;
        final long lastAcceptedVersion;
        final MetaData metaData;

        private OnDiskState(String nodeId, long currentTerm, long lastAcceptedVersion, MetaData metaData) {
            this.nodeId = nodeId;
            this.currentTerm = currentTerm;
            this.lastAcceptedVersion = lastAcceptedVersion;
            this.metaData = metaData;
        }
    }

    private OnDiskState loadBestOnDiskState() throws IOException {
        long maxCurrentTerm = 0L;
        String committedClusterUuid = null;
        OnDiskState bestOnDiskState = new OnDiskState(null, 0L, 0L, MetaData.EMPTY_META_DATA);

        // We use a write-all-read-one strategy: metadata is written to every data path when accepting it, which means it is mostly
        // sufficient to read _any_ copy. "Mostly" sufficient because the user can change the set of data paths when restarting, and may
        // add a data path containing a stale copy of the metadata. We deal with this by using the freshest copy we can find.
        for (final Path dataPath : nodeEnvironment.nodeDataPaths()) {
            for (int majorVersion = Version.CURRENT.major - 1; majorVersion <= Version.CURRENT.major; majorVersion++) {
                final Path indexPath = getMetaDataIndexPath(dataPath, majorVersion);
                if (Files.exists(indexPath)) {
                    try (Directory directory = new SimpleFSDirectory(indexPath);
                         DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                        final OnDiskState onDiskState = loadOnDiskState(directoryReader);

                        if (nodeEnvironment.nodeId().equals(onDiskState.nodeId) == false) {
                            throw new IllegalStateException("unexpected node ID in metadata, found [" + onDiskState.nodeId +
                                "] but expected [" + nodeEnvironment.nodeId() + "]");
                        }

                        if (onDiskState.metaData.clusterUUIDCommitted()) {
                            if (committedClusterUuid == null) {
                                committedClusterUuid = onDiskState.metaData.clusterUUID();
                            } else if (committedClusterUuid.equals(onDiskState.metaData.clusterUUID()) == false) {
                                throw new IllegalStateException("mismatched cluster UUIDs in metadata, found [" + committedClusterUuid +
                                    "] and [" + onDiskState.metaData.clusterUUID() +
                                    "]");
                            }
                        }

                        maxCurrentTerm = Math.max(maxCurrentTerm, onDiskState.currentTerm);

                        long acceptedTerm = onDiskState.metaData.coordinationMetaData().term();
                        long maxAcceptedTerm = bestOnDiskState.metaData.coordinationMetaData().term();
                        if (acceptedTerm > maxAcceptedTerm
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
        }

        if (bestOnDiskState.currentTerm != maxCurrentTerm) {
            throw new IllegalStateException("inconsistent terms found: best state is in term [" + bestOnDiskState.currentTerm +
                "] but there is a stale state with greater term [" + maxCurrentTerm + "]");
        }

        return bestOnDiskState;
    }

    private static Path getMetaDataIndexPath(Path path, int majorVersion) {
        return path.resolve(getMetaDataIndexDirectoryName(majorVersion));
    }

    public static String getMetaDataIndexDirectoryName(int majorVersion) {
        // include the version in the directory name to create a completely new index when upgrading to the next major version.
        return "_metadata_v" + majorVersion;
    }

    private OnDiskState loadOnDiskState(DirectoryReader reader) throws IOException {

        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);

        final SetOnce<MetaData.Builder> builderReference = new SetOnce<>();
        consumeFromType(searcher, GLOBAL_TYPE_NAME, bytes ->
        {
            final MetaData metaData = MetaData.fromXContent(XContentFactory.xContent(XContentType.SMILE)
                .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, bytes));
            logger.trace("found global metadata with last-accepted term [{}]", metaData.coordinationMetaData().term());
            builderReference.set(MetaData.builder(metaData));
        });

        final MetaData.Builder builder = builderReference.get();
        assert builder != null : "no global metadata found";

        logger.trace("got global metadata, now reading index metadata");

        final Set<String> indexUUIDsForAssertions = new HashSet<>();
        consumeFromType(searcher, INDEX_TYPE_NAME, bytes ->
        {
            final IndexMetaData indexMetaData = IndexMetaData.fromXContent(XContentFactory.xContent(XContentType.SMILE)
                .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, bytes));
            logger.trace("found index metadata for {}", indexMetaData.getIndex());
            //noinspection AssertWithSideEffects
            assert indexUUIDsForAssertions.add(indexMetaData.getIndexUUID());
            builder.put(indexMetaData, false);
        });

        final Map<String, String> userData = reader.getIndexCommit().getUserData();
        logger.trace("loaded metadata [{}] from [{}]", userData, reader.directory());
        assert userData.size() == 4 : userData;
        assert userData.get(CURRENT_TERM_KEY) != null;
        assert userData.get(LAST_ACCEPTED_VERSION_KEY) != null;
        assert userData.get(NODE_ID_KEY) != null;
        assert userData.get(NODE_VERSION_KEY) != null;
        return new OnDiskState(userData.get(NODE_ID_KEY), Long.parseLong(userData.get(CURRENT_TERM_KEY)),
            Long.parseLong(userData.get(LAST_ACCEPTED_VERSION_KEY)), builder.build());
    }

    private static void consumeFromType(IndexSearcher indexSearcher, String type,
                                        CheckedConsumer<byte[], IOException> docValuesConsumer)
        throws IOException {

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
                        docValuesConsumer.accept(
                            indexSearcher.getIndexReader().document(docIdSetIterator.docID()).getBinaryValue(DATA_FIELD_NAME).bytes);
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

    private static class MetaDataIndex implements Closeable {

        private final Logger logger;
        private final Directory directory;
        private final IndexWriter indexWriter;

        MetaDataIndex(Directory directory, IndexWriter indexWriter) {
            this.directory = directory;
            this.indexWriter = indexWriter;
            logger = Loggers.getLogger(MetaDataIndex.class, directory.toString());
        }

        void persistInitialState(String nodeId, long currentTerm, ClusterState lastAcceptedState) throws IOException {
            // Write the whole state out again to be sure it's fresh. Called during initialisation, so throwing an IOException is enough
            // to halt the node.

            // In the common case it's actually sufficient to commit() the existing state and not do any indexing. For instance, this is
            // true if there's only one data path on this master node, and the commit we just loaded was already written out by this
            // version of Elasticsearch. TODO TBD should we avoid indexing when possible?

            overwriteMetaData(lastAcceptedState.metaData());
            persist(nodeId, currentTerm, lastAcceptedState.version());
        }

        void overwriteMetaData(MetaData metaData) throws IOException {
            logger.trace("clearing existing metadata");
            indexWriter.deleteAll();

            logger.trace("adding global metadata doc");
            addGlobalMetaData(metaData);

            for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
                logger.trace("adding metadata doc for {}", cursor.value.getIndex());
                addIndexMetaData(cursor.value);
            }

            // Flush, to try and expose a failure (e.g. out of disk space) before committing, because we can handle a failure here more
            // gracefully than one that occurs during the commit process.
            indexWriter.flush();
        }

        void updateClusterState(ClusterState oldState, ClusterState newState) throws IOException {
            assert oldState.term() == newState.term();
            logger.trace("currentTerm [{}] matches previous currentTerm, writing changes only", newState.term());

            deleteGlobalMetaData();
            addGlobalMetaData(newState.metaData());

            final Map<String, Long> indexMetadataVersionByUUID = new HashMap<>(oldState.metaData().indices().size());
            for (ObjectCursor<IndexMetaData> cursor : oldState.metaData().indices().values()) {
                final IndexMetaData indexMetaData = cursor.value;
                final Long previousValue
                    = indexMetadataVersionByUUID.putIfAbsent(indexMetaData.getIndexUUID(), indexMetaData.getVersion());
                assert previousValue == null : indexMetaData.getIndexUUID() + " already mapped to " + previousValue;
            }

            for (ObjectCursor<IndexMetaData> cursor : newState.metaData().indices().values()) {
                final IndexMetaData indexMetaData = cursor.value;
                final Long previousVersion = indexMetadataVersionByUUID.get(indexMetaData.getIndexUUID());
                if (previousVersion == null || indexMetaData.getVersion() != previousVersion) {
                    if (previousVersion != null) {
                        deleteIndexMetaData(indexMetaData);
                        logger.trace("overwriting metadata for [{}], changing lastAcceptedVersion from [{}] to [{}]",
                            indexMetaData.getIndex(), previousVersion, indexMetaData.getVersion());
                    } else {
                        logger.trace("writing metadata for new [{}]", indexMetaData.getIndex());
                    }
                    addIndexMetaData(indexMetaData);
                } else {
                    logger.trace("no action required for [{}]", indexMetaData.getIndex());
                }
                indexMetadataVersionByUUID.remove(indexMetaData.getIndexUUID());
            }

            for (final String removedIndexUUID : indexMetadataVersionByUUID.keySet()) {
                logger.trace("removing metadata for [{}]", removedIndexUUID);
                deleteIndexMetaData(removedIndexUUID);
            }

            // Flush, to try and expose a failure (e.g. out of disk space) before committing, because we can handle a failure here more
            // gracefully than one that occurs during the commit process.
            indexWriter.flush();
        }

        void persist(String nodeId, long currentTerm, long lastAcceptedVersion) throws IOException {
            final Map<String, String> commitData = new HashMap<>(2);
            commitData.put(CURRENT_TERM_KEY, Long.toString(currentTerm));
            commitData.put(LAST_ACCEPTED_VERSION_KEY, Long.toString(lastAcceptedVersion));
            commitData.put(NODE_VERSION_KEY, Integer.toString(Version.CURRENT.id));
            commitData.put(NODE_ID_KEY, nodeId);
            indexWriter.setLiveCommitData(commitData.entrySet());
            indexWriter.commit();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(indexWriter, directory);
        }

        private void deleteGlobalMetaData() throws IOException {
            indexWriter.deleteDocuments(new Term(TYPE_FIELD_NAME, GLOBAL_TYPE_NAME));
        }

        private void addGlobalMetaData(MetaData metaData) throws IOException {
            indexWriter.addDocument(buildDocument(GLOBAL_TYPE_NAME, metaData));
        }

        private void deleteIndexMetaData(IndexMetaData indexMetaData) throws IOException {
            deleteIndexMetaData(indexMetaData.getIndexUUID());
        }

        private void deleteIndexMetaData(String indexUUID) throws IOException {
            indexWriter.deleteDocuments(new Term(INDEX_UUID_FIELD_NAME, indexUUID));
        }

        private void addIndexMetaData(IndexMetaData metaData) throws IOException {
            final Document document = buildDocument(INDEX_TYPE_NAME, metaData);
            assert metaData.getIndexUUID().equals(IndexMetaData.INDEX_UUID_NA_VALUE) == false;
            document.add(new StringField(INDEX_UUID_FIELD_NAME, metaData.getIndexUUID(), Field.Store.NO));
            indexWriter.addDocument(document);
        }

        private Document buildDocument(String typeName, ToXContent metaData) throws IOException {
            final Document document = new Document();
            document.add(new StringField(TYPE_FIELD_NAME, typeName, Field.Store.NO));

            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.SMILE, outputStream)) {
                xContentBuilder.startObject();
                metaData.toXContent(xContentBuilder, FORMAT_PARAMS);
                xContentBuilder.endObject();
            }
            document.add(new StoredField(DATA_FIELD_NAME, new BytesRef(outputStream.toByteArray())));
            return document;
        }
    }

    static class LucenePersistedState implements CoordinationState.PersistedState, Closeable {

        private long currentTerm;
        private ClusterState lastAcceptedState;
        private final List<MetaDataIndex> metaDataIndices;
        private final String nodeId;

        LucenePersistedState(String nodeId, List<MetaDataIndex> metaDataIndices, long currentTerm, ClusterState lastAcceptedState) {
            this.currentTerm = currentTerm;
            this.lastAcceptedState = lastAcceptedState;
            this.metaDataIndices = metaDataIndices;
            this.nodeId = nodeId;
        }

        @Override
        public long getCurrentTerm() {
            return currentTerm;
        }

        @Override
        public ClusterState getLastAcceptedState() {
            return lastAcceptedState;
        }

        void persistInitialState() throws IOException {
            for (MetaDataIndex metaDataIndex : metaDataIndices) {
                metaDataIndex.persistInitialState(nodeId, currentTerm, lastAcceptedState);
            }
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            persist(currentTerm, lastAcceptedState.version());
            this.currentTerm = currentTerm;
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            try {
                if (clusterState.term() != lastAcceptedState.term()) {
                    assert clusterState.term() > lastAcceptedState.term() : clusterState.term() + " vs " + lastAcceptedState.term();
                    // In a new currentTerm, we cannot compare the persisted metadata's lastAcceptedVersion to those in the new state, so
                    // it's simplest just to write everything again.
                    for (MetaDataIndex metaDataIndex : metaDataIndices) {
                        metaDataIndex.overwriteMetaData(clusterState.metaData());
                    }
                } else {
                    // Within the same currentTerm, we _can_ use metadata versions to skip unnecessary writing.
                    for (MetaDataIndex metaDataIndex : metaDataIndices) {
                        metaDataIndex.updateClusterState(lastAcceptedState, clusterState);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            persist(currentTerm, clusterState.version());
            lastAcceptedState = clusterState;
        }

        private void persist(long currentTerm, long lastAcceptedVersion) {
            try {
                for (MetaDataIndex metaDataIndex : metaDataIndices) {
                    metaDataIndex.persist(nodeId, currentTerm, lastAcceptedVersion);
                }
            } catch (IOException e) {
                // The persist() call has similar semantics to a fsync(): although it's atomic, if it fails then we've no idea whether the
                // data on disk is now the old version or the new version. It's safest to fail the whole node and retry from the beginning.
                throw new IOError(e);
            }
        }

        @Override
        public void close() throws IOException {
            logger.trace("closing");
            IOUtils.close(metaDataIndices);
        }
    }
}
