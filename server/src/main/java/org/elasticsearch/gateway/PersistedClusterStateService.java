/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

/**
 * Stores cluster metadata in a bare Lucene index (per data path) split across a number of documents. This is used by master-eligible nodes
 * to record the last-accepted cluster state during publication. The metadata is written incrementally where possible, leaving alone any
 * documents that have not changed. The index has the following fields:
 *
 * +--------------------------------+-------------------+----------------------------------------------+--------+-------------+
 * | "type" (string field)          | ID (string) field | "data" (stored binary field in SMILE format) | "page" | "last_page" |
 * +--------------------------------+-------------------+----------------------------------------------+--------+-------------+
 * | GLOBAL_TYPE_NAME  == "global"  | (none)            | Global metadata                              | large docs are       |
 * | INDEX_TYPE_NAME   == "index"   | "index_uuid"      | Index metadata                               | split into pages     |
 * | MAPPING_TYPE_NAME == "mapping" | "mapping_hash"    | Mapping metadata                             |                      |
 * +--------------------------------+-------------------+----------------------------------------------+--------+-------------+
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
 * | CLUSTER_UUID_KEY          | "cluster_uuid"          | The cluster uuid, null otherwise                                              |
 * | CLUSTER_UUID_COMMITTED_KEY| "cluster_uuid_committed"| Whether or not the cluster uuid was committed                                 |
 * +---------------------------+-------------------------+-------------------------------------------------------------------------------+
 *
 * (the last-accepted term is recorded in Metadata → CoordinationMetadata so does not need repeating here)
 */
public class PersistedClusterStateService {
    private static final Logger logger = LogManager.getLogger(PersistedClusterStateService.class);
    private static final String CURRENT_TERM_KEY = "current_term";
    private static final String LAST_ACCEPTED_VERSION_KEY = "last_accepted_version";
    private static final String NODE_ID_KEY = "node_id";
    private static final String CLUSTER_UUID_KEY = "cluster_uuid";
    private static final String CLUSTER_UUID_COMMITTED_KEY = "cluster_uuid_committed";
    private static final String OVERRIDDEN_NODE_VERSION_KEY = "overridden_node_version";
    static final String NODE_VERSION_KEY = "node_version";
    private static final String OLDEST_INDEX_VERSION_KEY = "oldest_index_version";
    public static final String TYPE_FIELD_NAME = "type";
    public static final String GLOBAL_TYPE_NAME = "global";
    public static final String INDEX_TYPE_NAME = "index";
    public static final String MAPPING_TYPE_NAME = "mapping";
    public static final String PROJECT_ID_FIELD_NAME = "project_id";
    private static final String DATA_FIELD_NAME = "data";
    private static final String INDEX_UUID_FIELD_NAME = "index_uuid";
    private static final String MAPPING_HASH_FIELD_NAME = "mapping_hash";
    public static final String PAGE_FIELD_NAME = "page";
    public static final String LAST_PAGE_FIELD_NAME = "last_page";
    public static final int IS_LAST_PAGE = 1;
    public static final int IS_NOT_LAST_PAGE = 0;
    private static final int COMMIT_DATA_SIZE = 7;

    private static final MergePolicy NO_MERGE_POLICY = noMergePolicy();
    private static final MergePolicy DEFAULT_MERGE_POLICY = defaultMergePolicy();

    public static final String METADATA_DIRECTORY_NAME = MetadataStateFormat.STATE_DIR_NAME;

    public static final Setting<TimeValue> SLOW_WRITE_LOGGING_THRESHOLD = Setting.timeSetting(
        "gateway.slow_write_logging_threshold",
        TimeValue.timeValueSeconds(10),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> DOCUMENT_PAGE_SIZE = Setting.byteSizeSetting(
        "cluster_state.document_page_size",
        ByteSizeValue.ofMb(1),
        ByteSizeValue.ONE,
        ByteSizeValue.ofGb(1),
        Setting.Property.NodeScope
    );

    private final Path[] dataPaths;
    private final String nodeId;
    private final XContentParserConfiguration parserConfig;
    private final LongSupplier relativeTimeMillisSupplier;
    private final ByteSizeValue documentPageSize;

    private volatile TimeValue slowWriteLoggingThreshold;
    // Whether the cluster has multi-project mode enabled, see also ProjectResolver#supportsMultipleProjects
    private final BooleanSupplier supportsMultipleProjects;

    public PersistedClusterStateService(
        NodeEnvironment nodeEnvironment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeMillisSupplier,
        BooleanSupplier supportsMultipleProjects
    ) {
        this(
            nodeEnvironment.nodeDataPaths(),
            nodeEnvironment.nodeId(),
            namedXContentRegistry,
            clusterSettings,
            relativeTimeMillisSupplier,
            supportsMultipleProjects
        );
    }

    public PersistedClusterStateService(
        Path[] dataPaths,
        String nodeId,
        NamedXContentRegistry namedXContentRegistry,
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeMillisSupplier,
        BooleanSupplier supportsMultipleProjects
    ) {
        this.dataPaths = dataPaths;
        this.nodeId = nodeId;
        this.parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE)
            .withRegistry(namedXContentRegistry);
        this.relativeTimeMillisSupplier = relativeTimeMillisSupplier;
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        this.supportsMultipleProjects = supportsMultipleProjects;
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
        this.documentPageSize = clusterSettings.get(DOCUMENT_PAGE_SIZE);
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
        final List<MetadataIndexWriter> metadataIndexWriters = new ArrayList<>();
        final List<Closeable> closeables = new ArrayList<>();
        boolean success = false;
        try {
            for (final Path path : dataPaths) {
                final Directory directory = createDirectory(path.resolve(METADATA_DIRECTORY_NAME));
                closeables.add(directory);

                final IndexWriter indexWriter = createIndexWriter(directory, false);
                closeables.add(indexWriter);
                metadataIndexWriters.add(new MetadataIndexWriter(path, directory, indexWriter, supportsMultipleProjects.getAsBoolean()));
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(closeables);
            }
        }
        return new Writer(
            metadataIndexWriters,
            nodeId,
            documentPageSize,
            relativeTimeMillisSupplier,
            () -> slowWriteLoggingThreshold,
            getAssertOnCommit()
        );
    }

    CheckedBiConsumer<Path, DirectoryReader, IOException> getAssertOnCommit() {
        return Assertions.ENABLED ? this::loadOnDiskState : null;
    }

    private static IndexWriter createIndexWriter(Directory directory, boolean openExisting) throws IOException {
        final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setInfoStream(InfoStream.NO_OUTPUT);
        // start empty since we re-write the whole cluster state to ensure it is all using the same format version
        indexWriterConfig.setOpenMode(openExisting ? IndexWriterConfig.OpenMode.APPEND : IndexWriterConfig.OpenMode.CREATE);
        // only commit when specifically instructed, we must not write any intermediate states
        indexWriterConfig.setCommitOnClose(false);
        // most of the data goes into stored fields which are not buffered, so each doc written accounts for ~500B of indexing buffer
        // (see e.g. BufferedUpdates#BYTES_PER_DEL_TERM); a 1MB buffer therefore gets flushed every ~2000 docs.
        indexWriterConfig.setRAMBufferSizeMB(1.0);
        // merge on the write thread (e.g. while flushing)
        indexWriterConfig.setMergeScheduler(new SerialMergeScheduler());
        // apply the adjusted merge policy
        indexWriterConfig.setMergePolicy(DEFAULT_MERGE_POLICY);

        return new IndexWriter(directory, indexWriterConfig);
    }

    /**
     * Remove all persisted cluster states from the given data paths, for use in tests. Should only be called when there is no open
     * {@link Writer} on these paths.
     */
    public static void deleteAll(Path[] dataPaths) throws IOException {
        for (Path dataPath : dataPaths) {
            Lucene.cleanLuceneIndex(new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)));
        }
    }

    protected Directory createDirectory(Path path) throws IOException {
        // it is possible to disable the use of MMapDirectory for indices, and it may be surprising to users that have done so if we still
        // use a MMapDirectory here, which might happen with FSDirectory.open(path), so we force an NIOFSDirectory to be on the safe side.
        return new NIOFSDirectory(path);
    }

    public Path[] getDataPaths() {
        return dataPaths;
    }

    public static class OnDiskState {
        private static final OnDiskState NO_ON_DISK_STATE = new OnDiskState(null, null, 0L, 0L, null, false, Metadata.EMPTY_METADATA);

        private final String nodeId;
        private final Path dataPath;
        public final long currentTerm;
        public final long lastAcceptedVersion;
        @Nullable
        public final String clusterUUID;
        @Nullable
        public final Boolean clusterUUIDCommitted;
        public final Metadata metadata;

        private OnDiskState(
            String nodeId,
            Path dataPath,
            long currentTerm,
            long lastAcceptedVersion,
            String clusterUUID,
            Boolean clusterUUIDCommitted,
            Metadata metadata
        ) {
            this.nodeId = nodeId;
            this.dataPath = dataPath;
            this.currentTerm = currentTerm;
            this.lastAcceptedVersion = lastAcceptedVersion;
            this.clusterUUID = clusterUUID;
            this.clusterUUIDCommitted = clusterUUIDCommitted;
            this.metadata = metadata;
        }

        public boolean empty() {
            return this == NO_ON_DISK_STATE;
        }
    }

    public record OnDiskStateMetadata(
        long currentTerm,
        long lastAcceptedVersion,
        String nodeId,
        @Nullable String clusterUUID,
        @Nullable Boolean clusterUUIDCommitted
    ) {}

    /**
     * Returns the node metadata for the given data paths, and checks if the node ids are unique
     * @param dataPaths the data paths to scan
     */
    @Nullable
    public static NodeMetadata nodeMetadata(Path... dataPaths) throws IOException {
        String nodeId = null;
        BuildVersion version = null;
        IndexVersion oldestIndexVersion = IndexVersions.ZERO;
        for (final Path dataPath : dataPaths) {
            final Path indexPath = dataPath.resolve(METADATA_DIRECTORY_NAME);
            if (Files.exists(indexPath)) {
                try (DirectoryReader reader = DirectoryReader.open(new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)))) {
                    final Map<String, String> userData = reader.getIndexCommit().getUserData();
                    assert userData.get(NODE_VERSION_KEY) != null;

                    final String thisNodeId = userData.get(NODE_ID_KEY);
                    assert thisNodeId != null;
                    if (nodeId != null && nodeId.equals(thisNodeId) == false) {
                        throw new CorruptStateException(
                            "unexpected node ID in metadata, found [" + thisNodeId + "] in [" + dataPath + "] but expected [" + nodeId + "]"
                        );
                    } else if (nodeId == null) {
                        nodeId = thisNodeId;
                        version = BuildVersion.fromNodeMetadata(userData.get(NODE_VERSION_KEY));
                        if (userData.containsKey(OLDEST_INDEX_VERSION_KEY)) {
                            oldestIndexVersion = IndexVersion.fromId(Integer.parseInt(userData.get(OLDEST_INDEX_VERSION_KEY)));
                        } else {
                            oldestIndexVersion = IndexVersions.ZERO;
                        }
                    }
                } catch (IndexNotFoundException e) {
                    logger.debug(() -> format("no on-disk state at %s", indexPath), e);
                }
            }
        }
        if (nodeId == null) {
            return null;
        }
        return new NodeMetadata(nodeId, version, oldestIndexVersion);
    }

    /**
     * Overrides the version field for the metadata in the given data path
     */
    public static void overrideVersion(BuildVersion newVersion, Path... dataPaths) throws IOException {
        for (final Path dataPath : dataPaths) {
            final Path indexPath = dataPath.resolve(METADATA_DIRECTORY_NAME);
            if (Files.exists(indexPath)) {
                try (DirectoryReader reader = DirectoryReader.open(new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)))) {
                    final Map<String, String> userData = reader.getIndexCommit().getUserData();
                    assert userData.get(NODE_VERSION_KEY) != null;

                    try (IndexWriter indexWriter = createIndexWriter(new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)), true)) {
                        final Map<String, String> commitData = new HashMap<>(userData);
                        commitData.put(NODE_VERSION_KEY, newVersion.toNodeMetadata());
                        commitData.put(OVERRIDDEN_NODE_VERSION_KEY, Boolean.toString(true));
                        indexWriter.setLiveCommitData(commitData.entrySet());
                        indexWriter.commit();
                    }
                } catch (IndexNotFoundException e) {
                    logger.debug(() -> format("no on-disk state at %s", indexPath), e);
                }
            }
        }
    }

    /**
     * Loads the best available on-disk cluster state. Returns {@link OnDiskState#NO_ON_DISK_STATE} if no such state was found.
     */
    public OnDiskState loadBestOnDiskState() throws IOException {
        return loadBestOnDiskState(true);
    }

    /**
     * Loads the available on-disk cluster state. Returns {@link OnDiskState#NO_ON_DISK_STATE} if no such state was found.
     * @param checkClean whether to check the index for corruption before loading, only for tests
     */
    OnDiskState loadBestOnDiskState(boolean checkClean) throws IOException {
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
                try (Directory directory = createDirectory(indexPath)) {
                    if (checkClean) {

                        logger.debug("checking cluster state integrity in [{}]", indexPath);

                        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
                            final boolean isClean;
                            try (
                                PrintStream printStream = new PrintStream(outputStream, true, StandardCharsets.UTF_8);
                                CheckIndex checkIndex = new CheckIndex(directory)
                            ) {
                                // Setting thread count to 1 prevents Lucene from starting disposable threads to execute the check and runs
                                // the check on this thread which is potentially faster for a small index like the cluster state and saves
                                // resources during test execution
                                checkIndex.setThreadCount(1);
                                checkIndex.setInfoStream(printStream);
                                checkIndex.setLevel(CheckIndex.Level.MIN_LEVEL_FOR_CHECKSUM_CHECKS);
                                isClean = checkIndex.checkIndex().clean;
                            }

                            if (isClean == false) {
                                if (logger.isErrorEnabled()) {
                                    outputStream.bytes().utf8ToString().lines().forEach(l -> logger.error("checkIndex: {}", l));
                                }
                                throw new CorruptStateException(
                                    "the index containing the cluster metadata under the data path ["
                                        + dataPath
                                        + "] has been changed by an external force after it was last written by Elasticsearch and is "
                                        + "now unreadable"
                                );
                            }
                        }
                    }

                    try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {

                        if (logger.isDebugEnabled()) {
                            final var indexCommit = directoryReader.getIndexCommit();
                            final var segmentsFileName = indexCommit.getSegmentsFileName();
                            try {
                                final var attributes = Files.readAttributes(indexPath.resolve(segmentsFileName), BasicFileAttributes.class);
                                logger.debug(
                                    "loading cluster state from commit ["
                                        + segmentsFileName
                                        + "] in ["
                                        + directory
                                        + "]: creationTime="
                                        + attributes.creationTime()
                                        + ", lastModifiedTime="
                                        + attributes.lastModifiedTime()
                                        + ", lastAccessTime="
                                        + attributes.lastAccessTime()
                                );
                            } catch (Exception e) {
                                logger.debug(
                                    "loading cluster state from commit ["
                                        + segmentsFileName
                                        + "] in ["
                                        + directory
                                        + "] but could not get file attributes",
                                    e
                                );
                            }
                            logger.debug("cluster state commit user data: {}", indexCommit.getUserData());

                            for (final var segmentCommitInfo : SegmentInfos.readCommit(directory, segmentsFileName)) {
                                logger.debug("loading cluster state from segment: {}", segmentCommitInfo);
                            }
                        }

                        final OnDiskState onDiskState = loadOnDiskState(dataPath, directoryReader);

                        if (nodeId.equals(onDiskState.nodeId) == false) {
                            throw new CorruptStateException(
                                "the index containing the cluster metadata under the data path ["
                                    + dataPath
                                    + "] belongs to a node with ID ["
                                    + onDiskState.nodeId
                                    + "] but this node's ID is ["
                                    + nodeId
                                    + "]"
                            );
                        }

                        if (onDiskState.metadata.clusterUUIDCommitted()) {
                            if (committedClusterUuid == null) {
                                committedClusterUuid = onDiskState.metadata.clusterUUID();
                                committedClusterUuidPath = dataPath;
                            } else if (committedClusterUuid.equals(onDiskState.metadata.clusterUUID()) == false) {
                                throw new CorruptStateException(
                                    "mismatched cluster UUIDs in metadata, found ["
                                        + committedClusterUuid
                                        + "] in ["
                                        + committedClusterUuidPath
                                        + "] and ["
                                        + onDiskState.metadata.clusterUUID()
                                        + "] in ["
                                        + dataPath
                                        + "]"
                                );
                            }
                        }

                        if (maxCurrentTermOnDiskState.empty() || maxCurrentTermOnDiskState.currentTerm < onDiskState.currentTerm) {
                            maxCurrentTermOnDiskState = onDiskState;
                        }

                        long acceptedTerm = onDiskState.metadata.coordinationMetadata().term();
                        long maxAcceptedTerm = bestOnDiskState.metadata.coordinationMetadata().term();
                        if (bestOnDiskState.empty()
                            || acceptedTerm > maxAcceptedTerm
                            || (acceptedTerm == maxAcceptedTerm
                                && (onDiskState.lastAcceptedVersion > bestOnDiskState.lastAcceptedVersion
                                    || (onDiskState.lastAcceptedVersion == bestOnDiskState.lastAcceptedVersion)
                                        && onDiskState.currentTerm > bestOnDiskState.currentTerm))) {
                            bestOnDiskState = onDiskState;
                        }
                    }
                } catch (IndexNotFoundException e) {
                    logger.debug(() -> format("no on-disk state at %s", indexPath), e);
                }
            }
        }

        if (bestOnDiskState.currentTerm != maxCurrentTermOnDiskState.currentTerm) {
            throw new CorruptStateException(
                "inconsistent terms found: best state is from ["
                    + bestOnDiskState.dataPath
                    + "] in term ["
                    + bestOnDiskState.currentTerm
                    + "] but there is a stale state in ["
                    + maxCurrentTermOnDiskState.dataPath
                    + "] with greater term ["
                    + maxCurrentTermOnDiskState.currentTerm
                    + "]"
            );
        }

        return bestOnDiskState;
    }

    public OnDiskState loadOnDiskState(Path dataPath, DirectoryReader reader) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);

        final SetOnce<Metadata.Builder> builderReference = new SetOnce<>();
        consumeFromType(searcher, GLOBAL_TYPE_NAME, ignored -> GLOBAL_TYPE_NAME, (doc, bytes) -> {
            final Metadata metadata = readXContent(bytes, Metadata.Builder::fromXContent);
            logger.trace("found global metadata with last-accepted term [{}]", metadata.coordinationMetadata().term());
            if (builderReference.get() != null) {
                throw new CorruptStateException("duplicate global metadata found in [" + dataPath + "]");
            }
            builderReference.set(Metadata.builder(metadata));
        });

        final Metadata.Builder builder = builderReference.get();
        if (builder == null) {
            throw new CorruptStateException("no global metadata found in [" + dataPath + "]");
        }

        logger.trace("got global metadata, now reading mapping metadata");

        final Map<ProjectId, Map<String, MappingMetadata>> mappingsByHash = new HashMap<>();
        consumeFromType(searcher, MAPPING_TYPE_NAME, document -> document.getField(MAPPING_HASH_FIELD_NAME).stringValue(), (doc, bytes) -> {
            final var mappingMetadata = readXContent(bytes, parser -> {
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new CorruptStateException(
                        "invalid mapping metadata: expected START_OBJECT but got [" + parser.currentToken() + "]"
                    );
                }
                if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                    throw new CorruptStateException(
                        "invalid mapping metadata: expected FIELD_NAME but got [" + parser.currentToken() + "]"
                    );
                }
                final var fieldName = parser.currentName();
                if ("content".equals(fieldName) == false) {
                    throw new CorruptStateException("invalid mapping metadata: unknown field [" + fieldName + "]");
                }
                if (parser.nextToken() != XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                    throw new CorruptStateException(
                        "invalid mapping metadata: expected VALUE_EMBEDDED_OBJECT but got [" + parser.currentToken() + "]"
                    );
                }
                return new MappingMetadata(new CompressedXContent(parser.binaryValue()));
            });
            final var projectId = ProjectId.ofNullable(doc.get(PROJECT_ID_FIELD_NAME), Metadata.DEFAULT_PROJECT_ID);
            final var hash = mappingMetadata.source().getSha256();
            logger.trace("found mapping metadata with hash {}", hash);
            if (mappingsByHash.computeIfAbsent(projectId, k -> new HashMap<>()).put(hash, mappingMetadata) != null) {
                throw new CorruptStateException(
                    "duplicate metadata found for mapping hash [" + hash + "] in project [" + projectId.id() + "]"
                );
            }
        });

        logger.trace("got metadata for [{}] mappings, now reading index metadata", mappingsByHash.size());

        final Set<String> indexUUIDs = new HashSet<>();
        consumeFromType(searcher, INDEX_TYPE_NAME, document -> document.getField(INDEX_UUID_FIELD_NAME).stringValue(), (doc, bytes) -> {
            ProjectId projectId = ProjectId.ofNullable(doc.get(PROJECT_ID_FIELD_NAME), Metadata.DEFAULT_PROJECT_ID);
            final IndexMetadata indexMetadata = readXContent(bytes, parser -> {
                try {
                    return IndexMetadata.fromXContent(parser, mappingsByHash.getOrDefault(projectId, Map.of()));
                } catch (Exception e) {
                    throw new CorruptStateException(e);
                }
            });
            logger.trace("found index metadata for {}", indexMetadata.getIndex());
            if (indexUUIDs.add(indexMetadata.getIndexUUID()) == false) {
                throw new CorruptStateException("duplicate metadata found for " + indexMetadata.getIndex() + " in [" + dataPath + "]");
            }
            builder.getProject(projectId).put(indexMetadata, false);
        });

        final Map<String, String> userData = reader.getIndexCommit().getUserData();
        logger.trace("loaded metadata [{}] from [{}]", userData, reader.directory());
        OnDiskStateMetadata onDiskStateMetadata = loadOnDiskStateMetadataFromUserData(userData);
        return new OnDiskState(
            onDiskStateMetadata.nodeId(),
            dataPath,
            onDiskStateMetadata.currentTerm(),
            onDiskStateMetadata.lastAcceptedVersion(),
            onDiskStateMetadata.clusterUUID(),
            onDiskStateMetadata.clusterUUIDCommitted(),
            builder.build()
        );
    }

    public OnDiskStateMetadata loadOnDiskStateMetadataFromUserData(Map<String, String> userData) {
        assert userData.get(CURRENT_TERM_KEY) != null;
        assert userData.get(LAST_ACCEPTED_VERSION_KEY) != null;
        assert userData.get(NODE_ID_KEY) != null;
        assert userData.get(NODE_VERSION_KEY) != null;
        assert userData.get(CLUSTER_UUID_KEY) != null;
        assert userData.get(CLUSTER_UUID_COMMITTED_KEY) != null;
        assert userData.get(OVERRIDDEN_NODE_VERSION_KEY) != null || userData.size() == COMMIT_DATA_SIZE;
        return new OnDiskStateMetadata(
            Long.parseLong(userData.get(CURRENT_TERM_KEY)),
            Long.parseLong(userData.get(LAST_ACCEPTED_VERSION_KEY)),
            userData.get(NODE_ID_KEY),
            userData.get(CLUSTER_UUID_KEY),
            userData.get(CLUSTER_UUID_COMMITTED_KEY) != null ? Boolean.parseBoolean(userData.get(CLUSTER_UUID_COMMITTED_KEY)) : null
        );
    }

    private <T> T readXContent(BytesReference bytes, CheckedFunction<XContentParser, T, IOException> reader) throws IOException {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(parserConfig, bytes, XContentType.SMILE)) {
            return reader.apply(parser);
        } catch (Exception e) {
            throw new CorruptStateException(e);
        }
    }

    private static void consumeFromType(
        IndexSearcher indexSearcher,
        String type,
        Function<Document, String> keyFunction,
        CheckedBiConsumer<Document, BytesReference, IOException> bytesReferenceConsumer
    ) throws IOException {

        final Query query = new TermQuery(new Term(TYPE_FIELD_NAME, type));
        final Weight weight = indexSearcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
        logger.trace("running query [{}]", query);

        final Map<String, PaginatedDocumentReader> documentReaders = new HashMap<>();

        for (LeafReaderContext leafReaderContext : indexSearcher.getIndexReader().leaves()) {
            logger.trace("new leafReaderContext: {}", leafReaderContext);
            final Scorer scorer = weight.scorer(leafReaderContext);
            if (scorer != null) {
                final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
                final IntPredicate isLiveDoc = liveDocs == null ? i -> true : liveDocs::get;
                final DocIdSetIterator docIdSetIterator = scorer.iterator();
                final StoredFields storedFields = leafReaderContext.reader().storedFields();
                while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    if (isLiveDoc.test(docIdSetIterator.docID())) {
                        logger.trace("processing doc {}", docIdSetIterator.docID());
                        final Document document = storedFields.document(docIdSetIterator.docID());
                        final BytesArray documentData = new BytesArray(document.getBinaryValue(DATA_FIELD_NAME));

                        if (document.getField(PAGE_FIELD_NAME) == null) {
                            // legacy format: not paginated or compressed
                            assert IndexVersions.MINIMUM_COMPATIBLE.before(IndexVersions.V_7_16_0);
                            bytesReferenceConsumer.accept(document, documentData);
                            continue;
                        }

                        final int pageIndex = document.getField(PAGE_FIELD_NAME).numericValue().intValue();
                        final boolean isLastPage = document.getField(LAST_PAGE_FIELD_NAME).numericValue().intValue() == IS_LAST_PAGE;

                        if (pageIndex == 0 && isLastPage) {
                            // common case: metadata fits in a single page
                            bytesReferenceConsumer.accept(document, uncompress(documentData));
                            continue;
                        }

                        // The metadata doesn't fit into a single page, so we accumulate pages until we have a complete set. Typically we
                        // will see pages in order since they were written in order, so the map will often have at most one entry. Also 1MB
                        // should be ample space for compressed index metadata so this is almost always used just for the global metadata.
                        // Even in pathological cases we shouldn't run out of memory here because we're doing this very early on in node
                        // startup, on the main thread and before most other services have started, and we will need space to serialize the
                        // whole cluster state in memory later on.

                        final var key = keyFunction.apply(document);
                        final PaginatedDocumentReader reader = documentReaders.computeIfAbsent(key, k -> new PaginatedDocumentReader());
                        final BytesReference bytesReference = reader.addPage(key, documentData, pageIndex, isLastPage);
                        if (bytesReference != null) {
                            documentReaders.remove(key);
                            bytesReferenceConsumer.accept(document, uncompress(bytesReference));
                        }
                    }
                }
            }
        }

        if (documentReaders.isEmpty() == false) {
            throw new CorruptStateException(
                "incomplete paginated documents " + documentReaders.keySet() + " when reading cluster state index [type=" + type + "]"
            );
        }
    }

    private static BytesReference uncompress(BytesReference bytesReference) throws IOException {
        try {
            return CompressorFactory.COMPRESSOR.uncompress(bytesReference);
        } catch (IOException e) {
            // no actual IO takes place, the data is all in-memory, so an exception indicates corruption
            throw new CorruptStateException(e);
        }
    }

    private static final ToXContent.Params FORMAT_PARAMS;

    static {
        Map<String, String> params = Maps.newMapWithExpectedSize(2);
        params.put("binary", "true");
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        params.put(Metadata.DEDUPLICATED_MAPPINGS_PARAM, Boolean.TRUE.toString());
        params.put("multi-project", "true");
        FORMAT_PARAMS = new ToXContent.MapParams(params);
    }

    @SuppressForbidden(reason = "merges are only temporarily suppressed, the merge scheduler does not need changing")
    private static MergePolicy noMergePolicy() {
        return NoMergePolicy.INSTANCE;
    }

    private static MergePolicy defaultMergePolicy() {
        final TieredMergePolicy mergePolicy = new TieredMergePolicy();

        // don't worry about cleaning up deletes too much, segments will often get completely deleted once they're old enough
        mergePolicy.setDeletesPctAllowed(50.0);
        // more/smaller segments means there's a better chance they just get deleted before needing a merge
        mergePolicy.setSegmentsPerTier(100);
        // ... but if we do end up merging them then do them all
        mergePolicy.setMaxMergeAtOnce(100);
        // always use compound segments to avoid fsync overhead
        mergePolicy.setNoCFSRatio(1.0);
        // segments are mostly tiny, so don't pretend they are bigger
        mergePolicy.setFloorSegmentMB(0.001);

        return mergePolicy;
    }

    /**
     * Encapsulates a single {@link IndexWriter} with its {@link Directory} for ease of closing, and a {@link Logger}. There is one of these
     * for each data path.
     */
    private static class MetadataIndexWriter implements Closeable {

        private final Logger logger;
        private final Path path;
        private final Directory directory;
        private final IndexWriter indexWriter;
        private final boolean supportsMultipleProjects;

        MetadataIndexWriter(Path path, Directory directory, IndexWriter indexWriter, boolean supportsMultipleProjects) {
            this.path = path;
            this.directory = directory;
            this.indexWriter = indexWriter;
            this.logger = Loggers.getLogger(MetadataIndexWriter.class, directory.toString());
            this.supportsMultipleProjects = supportsMultipleProjects;
        }

        void deleteAll() throws IOException {
            this.logger.trace("clearing existing metadata");
            indexWriter.deleteAll();
        }

        public void deleteGlobalMetadata() throws IOException {
            this.logger.trace("deleting global metadata docs");
            indexWriter.deleteDocuments(new Term(TYPE_FIELD_NAME, GLOBAL_TYPE_NAME));
        }

        void deleteIndexMetadata(String indexUUID) throws IOException {
            this.logger.trace("removing metadata for [{}]", indexUUID);
            indexWriter.deleteDocuments(new Term(INDEX_UUID_FIELD_NAME, indexUUID));
        }

        public void deleteMappingMetadata(ProjectId projectId, String mappingHash) throws IOException {
            this.logger.trace("removing mapping metadata for [{}] on project [{}]", mappingHash, projectId);

            if (supportsMultipleProjects) {
                // For the time-being, different projects can have the same mapping and the deduplication is per project.
                // Therefore, we must ensure the unused mapping is deleted for only the relevant project by adding project-id
                // to the query for deletion.
                // https://elasticco.atlassian.net/browse/ES-10605 tracks the future work for deduplicating mappings across projects
                final BooleanQuery matchProjectIdAndMappingHashKey = new BooleanQuery.Builder().add(
                    new TermQuery(new Term(PROJECT_ID_FIELD_NAME, projectId.id())),
                    BooleanClause.Occur.MUST
                ).add(new TermQuery(new Term(MAPPING_HASH_FIELD_NAME, mappingHash)), BooleanClause.Occur.MUST).build();
                indexWriter.deleteDocuments(matchProjectIdAndMappingHashKey);
            } else {
                // Stateful clusters or serverless clusters with multi-projects disabled have either just the mapping metadata (BWC)
                // or the mapping metadata for a single default project. For both cases, the unused mapping metadata
                // can be removed based on its hash since project-id does not come into play at all.
                assert Metadata.DEFAULT_PROJECT_ID.equals(projectId) : "expected default project id, got " + projectId;
                indexWriter.deleteDocuments(new Term(MAPPING_HASH_FIELD_NAME, mappingHash));
            }
        }

        void flush() throws IOException {
            this.logger.trace("flushing");
            this.indexWriter.flush();
        }

        void startWrite() {
            // Disable merges during indexing - many older segments will ultimately contain no live docs and simply get deleted.
            indexWriter.getConfig().setMergePolicy(NO_MERGE_POLICY);
        }

        void prepareCommit(
            String nodeId,
            long currentTerm,
            long lastAcceptedVersion,
            IndexVersion oldestIndexVersion,
            String clusterUUID,
            boolean clusterUUIDCommitted
        ) throws IOException {
            indexWriter.getConfig().setMergePolicy(DEFAULT_MERGE_POLICY);
            indexWriter.maybeMerge();

            final Map<String, String> commitData = Maps.newMapWithExpectedSize(COMMIT_DATA_SIZE);
            commitData.put(CURRENT_TERM_KEY, Long.toString(currentTerm));
            commitData.put(LAST_ACCEPTED_VERSION_KEY, Long.toString(lastAcceptedVersion));
            commitData.put(NODE_VERSION_KEY, BuildVersion.current().toNodeMetadata());
            commitData.put(OLDEST_INDEX_VERSION_KEY, Integer.toString(oldestIndexVersion.id()));
            commitData.put(NODE_ID_KEY, nodeId);
            commitData.put(CLUSTER_UUID_KEY, clusterUUID);
            commitData.put(CLUSTER_UUID_COMMITTED_KEY, Boolean.toString(clusterUUIDCommitted));
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

        private final List<MetadataIndexWriter> metadataIndexWriters;
        private final String nodeId;
        private final LongSupplier relativeTimeMillisSupplier;
        private final Supplier<TimeValue> slowWriteLoggingThresholdSupplier;

        boolean fullStateWritten = false;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final byte[] documentBuffer;
        @Nullable // if assertions disabled or we explicitly don't want to assert on commit in a test
        private final CheckedBiConsumer<Path, DirectoryReader, IOException> assertOnCommit;

        private Writer(
            List<MetadataIndexWriter> metadataIndexWriters,
            String nodeId,
            ByteSizeValue documentPageSize,
            LongSupplier relativeTimeMillisSupplier,
            Supplier<TimeValue> slowWriteLoggingThresholdSupplier,
            @Nullable // if assertions disabled or we explicitly don't want to assert on commit in a test
            CheckedBiConsumer<Path, DirectoryReader, IOException> assertOnCommit
        ) {
            this.metadataIndexWriters = metadataIndexWriters;
            this.nodeId = nodeId;
            this.relativeTimeMillisSupplier = relativeTimeMillisSupplier;
            this.slowWriteLoggingThresholdSupplier = slowWriteLoggingThresholdSupplier;
            this.documentBuffer = new byte[ByteSizeUnit.BYTES.toIntBytes(documentPageSize.getBytes())];
            this.assertOnCommit = assertOnCommit;
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
            if (metadataIndexWriters.stream()
                .map(writer -> writer.indexWriter)
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

                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.startWrite();
                }

                Metadata metadata = clusterState.metadata();
                final WriterStats stats = overwriteMetadata(metadata);
                commit(
                    currentTerm,
                    clusterState.version(),
                    metadata.oldestIndexVersionAllProjects(),
                    metadata.clusterUUID(),
                    metadata.clusterUUIDCommitted()
                );
                fullStateWritten = true;
                final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
                final TimeValue finalSlowWriteLoggingThreshold = slowWriteLoggingThresholdSupplier.get();
                if (durationMillis >= finalSlowWriteLoggingThreshold.getMillis()) {
                    logger.warn(
                        "writing full cluster state took [{}ms] which is above the warn threshold of [{}]; {}",
                        durationMillis,
                        finalSlowWriteLoggingThreshold,
                        stats
                    );
                } else {
                    logger.debug("writing full cluster state took [{}ms]; {}", durationMillis, stats);
                }
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
        }

        /**
         * Updates and commits the given cluster state update
         */
        void writeIncrementalStateAndCommit(long currentTerm, ClusterState previousClusterState, ClusterState clusterState)
            throws IOException {
            ensureOpen();
            ensureFullStateWritten();

            try {
                final long startTimeMillis = relativeTimeMillisSupplier.getAsLong();

                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.startWrite();
                }

                Metadata metadata = clusterState.metadata();
                final WriterStats stats = updateMetadata(previousClusterState.metadata(), metadata);
                commit(
                    currentTerm,
                    clusterState.version(),
                    metadata.oldestIndexVersionAllProjects(),
                    metadata.clusterUUID(),
                    metadata.clusterUUIDCommitted()
                );
                final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
                final TimeValue finalSlowWriteLoggingThreshold = slowWriteLoggingThresholdSupplier.get();
                if (durationMillis >= finalSlowWriteLoggingThreshold.getMillis()) {
                    logger.warn(
                        "writing cluster state took [{}ms] which is above the warn threshold of [{}]; {}",
                        durationMillis,
                        finalSlowWriteLoggingThreshold,
                        stats
                    );
                } else {
                    logger.debug("writing cluster state took [{}ms]; {}", durationMillis, stats);
                }
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
        }

        private void ensureFullStateWritten() {
            assert fullStateWritten : "Need to write full state first before doing incremental writes";
            // noinspection ConstantConditions to catch this even if assertions are disabled
            if (fullStateWritten == false) {
                logger.error("cannot write incremental state");
                throw new IllegalStateException("cannot write incremental state");
            }
        }

        /**
         * Update the persisted metadata to match the given cluster state by removing any stale or unnecessary documents and adding any
         * updated documents.
         */
        private WriterStats updateMetadata(Metadata previouslyWrittenMetadata, Metadata metadata) throws IOException {
            assert previouslyWrittenMetadata.coordinationMetadata().term() == metadata.coordinationMetadata().term();
            logger.trace("currentTerm [{}] matches previous currentTerm, writing changes only", metadata.coordinationMetadata().term());

            if (previouslyWrittenMetadata == metadata) {
                // breakout early if nothing changed
                return new WriterStats(
                    false,
                    false,
                    metadata.projects().values().stream().map(ProjectMetadata::getMappingsByHash).mapToInt(Map::size).sum(),
                    0,
                    0,
                    metadata.projects().values().stream().mapToInt(ProjectMetadata::size).sum(),
                    0,
                    0,
                    0
                );
            }
            final boolean updateGlobalMeta = Metadata.isGlobalStateEquals(previouslyWrittenMetadata, metadata) == false;
            if (updateGlobalMeta) {
                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.deleteGlobalMetadata();
                }

                addGlobalMetadataDocuments(metadata);
            }

            int numMappingsAdded = 0;
            int numMappingsRemoved = 0;
            int numMappingsUnchanged = 0;
            int numIndicesAdded = 0;
            int numIndicesUpdated = 0;
            int numIndicesRemoved = 0;
            int numIndicesUnchanged = 0;

            for (ProjectMetadata project : metadata.projects().values()) {
                final ProjectId projectId = project.id();
                ProjectMetadata previousProject = previouslyWrittenMetadata.projects().get(projectId);
                // If there's no previous project, this project is new so we add all indices and mappings.
                if (previousProject == null) {
                    for (final var entry : project.getMappingsByHash().entrySet()) {
                        addMappingDocuments(projectId, entry.getKey(), entry.getValue());
                        numMappingsAdded++;
                    }
                    for (IndexMetadata indexMetadata : project.indices().values()) {
                        addIndexMetadataDocuments(projectId, indexMetadata);
                        numIndicesAdded++;
                    }
                    continue;
                }
                // Determine changed or added mappings.
                final var previousMappingHashes = new HashSet<>(previousProject.getMappingsByHash().keySet());
                for (final var entry : project.getMappingsByHash().entrySet()) {
                    if (previousMappingHashes.remove(entry.getKey()) == false) {
                        addMappingDocuments(projectId, entry.getKey(), entry.getValue());
                        numMappingsAdded++;
                    } else {
                        logger.trace("no action required for mapping [{}]", entry.getKey());
                        numMappingsUnchanged++;
                    }
                }

                // Remove unused mappings.
                for (final var unusedMappingHash : previousMappingHashes) {
                    for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                        metadataIndexWriter.deleteMappingMetadata(projectId, unusedMappingHash);
                        numMappingsRemoved++;
                    }
                }

                // Determine changed or added indices.
                final Map<String, Long> indexMetadataVersionByUUID = Maps.newMapWithExpectedSize(previousProject.indices().size());
                previousProject.indices().forEach((name, indexMetadata) -> {
                    final Long previousValue = indexMetadataVersionByUUID.putIfAbsent(
                        indexMetadata.getIndexUUID(),
                        indexMetadata.getVersion()
                    );
                    assert previousValue == null : indexMetadata.getIndexUUID() + " already mapped to " + previousValue;
                });

                for (IndexMetadata indexMetadata : project.indices().values()) {
                    final Long previousVersion = indexMetadataVersionByUUID.get(indexMetadata.getIndexUUID());
                    if (previousVersion == null || indexMetadata.getVersion() != previousVersion) {
                        logger.trace(
                            "updating metadata for [{}], changing version from [{}] to [{}]",
                            indexMetadata.getIndex(),
                            previousVersion,
                            indexMetadata.getVersion()
                        );
                        if (previousVersion == null) {
                            numIndicesAdded++;
                        } else {
                            numIndicesUpdated++;
                        }

                        for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                            metadataIndexWriter.deleteIndexMetadata(indexMetadata.getIndexUUID());
                        }

                        addIndexMetadataDocuments(projectId, indexMetadata);
                    } else {
                        numIndicesUnchanged++;
                        logger.trace("no action required for index [{}]", indexMetadata.getIndex());
                    }
                    indexMetadataVersionByUUID.remove(indexMetadata.getIndexUUID());
                }

                // Remove unused indices.
                for (String removedIndexUUID : indexMetadataVersionByUUID.keySet()) {
                    for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                        numIndicesRemoved++;
                        metadataIndexWriter.deleteIndexMetadata(removedIndexUUID);
                    }
                }
            }

            // Remove all indices and mappings for removed projects.
            for (final var removedProject : previouslyWrittenMetadata.projects().values()) {
                if (metadata.projects().containsKey(removedProject.id())) {
                    continue;
                }
                for (final var unusedMappingHash : removedProject.getMappingsByHash().keySet()) {
                    for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                        metadataIndexWriter.deleteMappingMetadata(removedProject.id(), unusedMappingHash);
                        numMappingsRemoved++;
                    }
                }
                for (IndexMetadata removedIndexMetadata : removedProject.indices().values()) {
                    for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                        numIndicesRemoved++;
                        metadataIndexWriter.deleteIndexMetadata(removedIndexMetadata.getIndexUUID());
                    }
                }
            }

            // Flush, to try and expose a failure (e.g. out of disk space) before committing, because we can handle a failure here more
            // gracefully than one that occurs during the commit process.
            for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                metadataIndexWriter.flush();
            }

            return new WriterStats(
                false,
                updateGlobalMeta,
                numMappingsUnchanged,
                numMappingsAdded,
                numMappingsRemoved,
                numIndicesUnchanged,
                numIndicesAdded,
                numIndicesUpdated,
                numIndicesRemoved
            );
        }

        private static int lastPageValue(boolean isLastPage) {
            return isLastPage ? IS_LAST_PAGE : IS_NOT_LAST_PAGE;
        }

        private void addMappingDocuments(ProjectId projectId, String key, MappingMetadata mappingMetadata) throws IOException {
            logger.trace("writing mapping metadata with hash [{}] on project [{}]", key, projectId);
            writePages(
                (builder, params) -> builder.field("content", mappingMetadata.source().compressed()),
                (((bytesRef, pageIndex, isLastPage) -> {
                    final Document document = new Document();
                    document.add(new StringField(TYPE_FIELD_NAME, MAPPING_TYPE_NAME, Field.Store.NO));
                    document.add(new StringField(MAPPING_HASH_FIELD_NAME, key, Field.Store.YES));
                    document.add(new StoredField(PAGE_FIELD_NAME, pageIndex));
                    document.add(new StoredField(LAST_PAGE_FIELD_NAME, lastPageValue(isLastPage)));
                    document.add(new StoredField(DATA_FIELD_NAME, bytesRef));
                    document.add(new StringField(PROJECT_ID_FIELD_NAME, projectId.id(), Field.Store.YES));
                    for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                        metadataIndexWriter.indexWriter.addDocument(document);
                    }
                }))
            );
        }

        private void addIndexMetadataDocuments(ProjectId projectId, IndexMetadata indexMetadata) throws IOException {
            final String indexUUID = indexMetadata.getIndexUUID();
            assert indexUUID.equals(IndexMetadata.INDEX_UUID_NA_VALUE) == false;
            logger.trace("updating metadata for [{}] on project [{}]", indexMetadata.getIndex(), projectId);
            writePages(indexMetadata, ((bytesRef, pageIndex, isLastPage) -> {
                final Document document = new Document();
                document.add(new StringField(TYPE_FIELD_NAME, INDEX_TYPE_NAME, Field.Store.NO));
                document.add(new StringField(INDEX_UUID_FIELD_NAME, indexUUID, Field.Store.YES));
                document.add(new StoredField(PAGE_FIELD_NAME, pageIndex));
                document.add(new StoredField(LAST_PAGE_FIELD_NAME, lastPageValue(isLastPage)));
                document.add(new StoredField(DATA_FIELD_NAME, bytesRef));
                document.add(new StringField(PROJECT_ID_FIELD_NAME, projectId.id(), Field.Store.YES));
                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.indexWriter.addDocument(document);
                }
            }));
        }

        @FixForMultiProject
        private void addGlobalMetadataDocuments(Metadata metadata) throws IOException {
            logger.trace("updating global metadata doc");
            // TODO multi-project: we might want to write each project as a separate document instead of one huge document.
            writePages(ChunkedToXContent.wrapAsToXContent(metadata), (bytesRef, pageIndex, isLastPage) -> {
                final Document document = new Document();
                document.add(new StringField(TYPE_FIELD_NAME, GLOBAL_TYPE_NAME, Field.Store.NO));
                document.add(new StoredField(PAGE_FIELD_NAME, pageIndex));
                document.add(new StoredField(LAST_PAGE_FIELD_NAME, lastPageValue(isLastPage)));
                document.add(new StoredField(DATA_FIELD_NAME, bytesRef));
                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.indexWriter.addDocument(document);
                }
            });
        }

        private void writePages(ToXContent metadata, PageWriter pageWriter) throws IOException {
            try (
                PageWriterOutputStream paginatedStream = new PageWriterOutputStream(documentBuffer, pageWriter);
                OutputStream compressedStream = CompressorFactory.COMPRESSOR.threadLocalOutputStream(paginatedStream);
                XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.SMILE, compressedStream)
            ) {
                xContentBuilder.startObject();
                metadata.toXContent(xContentBuilder, FORMAT_PARAMS);
                xContentBuilder.endObject();
            }
        }

        /**
         * Update the persisted metadata to match the given cluster state by removing all existing documents and then adding new documents.
         */
        private WriterStats overwriteMetadata(Metadata metadata) throws IOException {
            for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                metadataIndexWriter.deleteAll();
            }
            return addMetadata(metadata);
        }

        /**
         * Add documents for the metadata of the given cluster state, assuming that there are currently no documents.
         */
        private WriterStats addMetadata(Metadata metadata) throws IOException {
            addGlobalMetadataDocuments(metadata);

            for (ProjectMetadata project : metadata.projects().values()) {
                for (final var entry : project.getMappingsByHash().entrySet()) {
                    addMappingDocuments(project.id(), entry.getKey(), entry.getValue());
                }

                for (IndexMetadata indexMetadata : project.indices().values()) {
                    addIndexMetadataDocuments(project.id(), indexMetadata);
                }
            }

            // Flush, to try and expose a failure (e.g. out of disk space) before committing, because we can handle a failure here more
            // gracefully than one that occurs during the commit process.
            for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                metadataIndexWriter.flush();
            }

            return new WriterStats(
                true,
                true,
                0,
                metadata.projects().values().stream().map(ProjectMetadata::getMappingsByHash).mapToInt(Map::size).sum(),
                0,
                0,
                metadata.projects().values().stream().mapToInt(ProjectMetadata::size).sum(),
                0,
                0
            );
        }

        public void writeIncrementalTermUpdateAndCommit(
            long currentTerm,
            long lastAcceptedVersion,
            IndexVersion oldestIndexVersion,
            String clusterUUID,
            boolean clusterUUIDCommitted
        ) throws IOException {
            ensureOpen();
            ensureFullStateWritten();
            commit(currentTerm, lastAcceptedVersion, oldestIndexVersion, clusterUUID, clusterUUIDCommitted);
        }

        void commit(
            long currentTerm,
            long lastAcceptedVersion,
            IndexVersion oldestIndexVersion,
            String clusterUUID,
            boolean clusterUUIDCommitted
        ) throws IOException {
            ensureOpen();
            prepareCommit(currentTerm, lastAcceptedVersion, oldestIndexVersion, clusterUUID, clusterUUIDCommitted);
            completeCommit();
            assert assertOnCommit();
        }

        private boolean assertOnCommit() {
            if (assertOnCommit != null && Randomness.get().nextInt(100) == 0) {
                // only rarely run this assertion since reloading the whole state can be quite expensive
                for (final var metadataIndexWriter : metadataIndexWriters) {
                    try (var directoryReader = DirectoryReader.open(metadataIndexWriter.indexWriter)) {
                        assertOnCommit.accept(metadataIndexWriter.path, directoryReader);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            }

            return true;
        }

        private void prepareCommit(
            long currentTerm,
            long lastAcceptedVersion,
            IndexVersion oldestIndexVersion,
            String clusterUUID,
            boolean clusterUUIDCommitted
        ) throws IOException {
            boolean prepareCommitSuccess = false;
            try {
                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.prepareCommit(
                        nodeId,
                        currentTerm,
                        lastAcceptedVersion,
                        oldestIndexVersion,
                        clusterUUID,
                        clusterUUIDCommitted
                    );
                }
                prepareCommitSuccess = true;
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
                if (prepareCommitSuccess == false) {
                    closeAndSuppressExceptions(); // let the error propagate even if closing fails here
                }
            }
        }

        private void completeCommit() {
            boolean commitSuccess = false;
            try {
                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.commit();
                }
                commitSuccess = true;
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
                if (commitSuccess == false) {
                    closeAndSuppressExceptions(); // let the error propagate even if closing fails here
                }
            }
        }

        private void closeAndSuppressExceptions() {
            if (closed.compareAndSet(false, true)) {
                logger.trace("closing PersistedClusterStateService.Writer suppressing any exceptions");
                IOUtils.closeWhileHandlingException(metadataIndexWriters);
            }
        }

        @Override
        public void close() throws IOException {
            logger.trace("closing PersistedClusterStateService.Writer");
            if (closed.compareAndSet(false, true)) {
                IOUtils.close(metadataIndexWriters);
            }
        }

        private record WriterStats(
            boolean isFullWrite,
            boolean globalMetaUpdated,
            int numMappingsUnchanged,
            int numMappingsAdded,
            int numMappingsRemoved,
            int numIndicesUnchanged,
            int numIndicesAdded,
            int numIndicesUpdated,
            int numIndicesRemoved
        ) {
            @Override
            public String toString() {
                if (isFullWrite) {
                    return String.format(
                        Locale.ROOT,
                        "wrote global metadata, [%d] mappings, and metadata for [%d] indices",
                        numMappingsAdded,
                        numIndicesAdded
                    );
                } else {
                    return String.format(
                        Locale.ROOT,
                        """
                            [%s] global metadata, \
                            wrote [%d] new mappings, removed [%d] mappings and skipped [%d] unchanged mappings, \
                            wrote metadata for [%d] new indices and [%d] existing indices, \
                            removed metadata for [%d] indices and skipped [%d] unchanged indices""",
                        globalMetaUpdated ? "wrote" : "skipped writing",
                        numMappingsAdded,
                        numMappingsRemoved,
                        numMappingsUnchanged,
                        numIndicesAdded,
                        numIndicesUpdated,
                        numIndicesRemoved,
                        numIndicesUnchanged
                    );
                }
            }
        }
    }

    private interface PageWriter {
        void consumePage(BytesRef bytesRef, int pageIndex, boolean isLastPage) throws IOException;
    }

    private static class PageWriterOutputStream extends OutputStream {

        private final byte[] buffer;
        private final PageWriter pageWriter;
        private int bufferPosition;
        private int pageIndex;
        private int bytesFlushed;
        private boolean closed;

        PageWriterOutputStream(byte[] buffer, PageWriter pageWriter) {
            assert buffer.length > 0;
            this.buffer = buffer;
            this.pageWriter = pageWriter;
        }

        @Override
        public void write(@SuppressWarnings("NullableProblems") byte[] b, int off, int len) throws IOException {
            assert closed == false : "cannot write after close";
            while (len > 0) {
                if (bufferPosition == buffer.length) {
                    flushPage(false);
                }
                assert bufferPosition < buffer.length;

                final int lenToBuffer = Math.min(len, buffer.length - bufferPosition);
                System.arraycopy(b, off, buffer, bufferPosition, lenToBuffer);
                bufferPosition += lenToBuffer;
                off += lenToBuffer;
                len -= lenToBuffer;
            }
        }

        @Override
        public void write(int b) throws IOException {
            assert closed == false : "cannot write after close";
            if (bufferPosition == buffer.length) {
                flushPage(false);
            }
            assert bufferPosition < buffer.length;
            buffer[bufferPosition++] = (byte) b;
        }

        @Override
        public void flush() throws IOException {
            assert closed == false : "must not flush after close";
            // keep buffering, don't actually flush anything
        }

        @Override
        public void close() throws IOException {
            if (closed == false) {
                closed = true;
                flushPage(true);
            }
        }

        private void flushPage(boolean isLastPage) throws IOException {
            assert bufferPosition > 0 : "cannot flush empty page";
            assert bufferPosition == buffer.length || isLastPage : "only the last page may be incomplete";
            if (bytesFlushed > Integer.MAX_VALUE - bufferPosition) {
                // At startup the state doc is loaded into a single BytesReference which means it must be no longer than Integer.MAX_VALUE,
                // so we would not be able to read it if we carried on. Better to fail early during writing instead.
                throw new IllegalArgumentException("cannot persist cluster state document larger than 2GB");
            }
            bytesFlushed += bufferPosition;
            pageWriter.consumePage(new BytesRef(buffer, 0, bufferPosition), pageIndex, isLastPage);
            pageIndex += 1;
            bufferPosition = 0;
        }
    }

    private static class PaginatedDocumentReader {

        private final ArrayList<BytesReference> pages = new ArrayList<>();
        private int emptyPages;
        private int pageCount = -1;

        /**
         * @return a {@link BytesReference} if all pages received, otherwise {@code null}.
         */
        @Nullable
        BytesReference addPage(String key, BytesReference bytesReference, int pageIndex, boolean isLastPage) throws CorruptStateException {
            while (pages.size() < pageIndex) {
                if (pageCount != -1) {
                    throw new CorruptStateException(
                        "found page ["
                            + pageIndex
                            + "] but last page was ["
                            + pageCount
                            + "] when reading key ["
                            + key
                            + "] from cluster state index"
                    );
                }
                emptyPages += 1;
                pages.add(null);
            }
            if (pages.size() == pageIndex) {
                pages.add(bytesReference);
            } else {
                if (pages.get(pageIndex) != null) {
                    throw new CorruptStateException(
                        "found duplicate page [" + pageIndex + "] when reading key [" + key + "] from cluster state index"
                    );
                }
                emptyPages -= 1;
                pages.set(pageIndex, bytesReference);
            }
            if (isLastPage) {
                if (pageCount != -1) {
                    throw new CorruptStateException(
                        "already read page count "
                            + pageCount
                            + " but page "
                            + pageIndex
                            + " is also marked as the last page when reading key ["
                            + key
                            + "] from cluster state index"
                    );
                }
                pageCount = pageIndex + 1;
                if (pages.size() != pageCount) {
                    throw new CorruptStateException(
                        "already read " + pages.size() + " pages but page " + pageIndex + " is marked as the last page"
                    );
                }
            }
            if (pageCount != -1 && emptyPages == 0) {
                return CompositeBytesReference.of(pages.toArray(new BytesReference[0]));
            } else {
                return null;
            }
        }
    }
}
