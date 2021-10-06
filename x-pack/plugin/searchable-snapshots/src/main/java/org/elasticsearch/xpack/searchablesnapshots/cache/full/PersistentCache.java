/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.full;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import static java.util.Collections.synchronizedMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSortedSet;
import static org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService.getShardCachePath;
import static org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService.resolveSnapshotCache;

public class PersistentCache implements Closeable {

    private static final Logger logger = LogManager.getLogger(PersistentCache.class);

    private static final String NODE_VERSION_COMMIT_KEY = "node_version";

    private final NodeEnvironment nodeEnvironment;
    private final Map<String, Document> documents;
    private final List<CacheIndexWriter> writers;
    private final AtomicBoolean started;
    private final AtomicBoolean closed;

    public PersistentCache(NodeEnvironment nodeEnvironment) {
        this.documents = synchronizedMap(loadDocuments(nodeEnvironment));
        this.writers = createWriters(nodeEnvironment);
        this.nodeEnvironment = nodeEnvironment;
        this.started = new AtomicBoolean();
        this.closed = new AtomicBoolean();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("Persistent cache is already closed");
        }
    }

    private void ensureStarted() {
        if (started.get() == false) {
            throw new IllegalStateException("Persistent cache is not started");
        }
    }

    /**
     * @return the {@link CacheIndexWriter} to use for the given {@link CacheFile}
     */
    private CacheIndexWriter getWriter(CacheFile cacheFile) {
        ensureOpen();
        if (writers.size() == 1) {
            return writers.get(0);
        } else {
            final Path path = cacheFile.getFile().toAbsolutePath();
            return writers.stream()
                .filter(writer -> path.startsWith(writer.nodePath().path))
                .findFirst()
                .orElseThrow(() -> new PersistentCacheIndexNotFoundException(nodeEnvironment, cacheFile));
        }
    }

    public void addCacheFile(CacheFile cacheFile, SortedSet<ByteRange> ranges) throws IOException {
        ensureStarted();
        getWriter(cacheFile).updateCacheFile(cacheFile, ranges);
    }

    public void removeCacheFile(CacheFile cacheFile) throws IOException {
        ensureStarted();
        getWriter(cacheFile).deleteCacheFile(cacheFile);
    }

    public long getCacheSize(ShardId shardId, SnapshotId snapshotId) {
        return getCacheSize(shardId, snapshotId, Files::exists);
    }

    // pkg private for tests
    long getCacheSize(ShardId shardId, SnapshotId snapshotId, Predicate<Path> predicate) {
        long aggregateSize = 0L;
        for (CacheIndexWriter writer : writers) {
            final Path snapshotCacheDir = resolveSnapshotCache(writer.nodePath().resolve(shardId)).resolve(snapshotId.getUUID());
            if (Files.exists(snapshotCacheDir) == false) {
                continue; // searchable snapshot shard is not present on this node path, not need to run a query
            }
            try (IndexReader indexReader = DirectoryReader.open(writer.indexWriter)) {
                final IndexSearcher searcher = new IndexSearcher(indexReader);
                searcher.setQueryCache(null);
                final Weight weight = searcher.createWeight(
                    new BooleanQuery.Builder().add(
                        new TermQuery(new Term(SNAPSHOT_ID_FIELD, snapshotId.getUUID())),
                        BooleanClause.Occur.MUST
                    )
                        .add(new TermQuery(new Term(SHARD_INDEX_ID_FIELD, shardId.getIndex().getUUID())), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term(SHARD_ID_FIELD, String.valueOf(shardId.getId()))), BooleanClause.Occur.MUST)
                        .build(),
                    ScoreMode.COMPLETE_NO_SCORES,
                    0.0f
                );
                for (LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
                    final Scorer scorer = weight.scorer(leafReaderContext);
                    if (scorer != null) {
                        final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
                        final IntPredicate isLiveDoc = liveDocs == null ? i -> true : liveDocs::get;
                        final DocIdSetIterator docIdSetIterator = scorer.iterator();
                        while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            if (isLiveDoc.test(docIdSetIterator.docID())) {
                                final Document document = leafReaderContext.reader().document(docIdSetIterator.docID());
                                final String cacheFileId = getValue(document, CACHE_ID_FIELD);
                                if (predicate.test(snapshotCacheDir.resolve(cacheFileId))) {
                                    long size = buildCacheFileRanges(document).stream().mapToLong(ByteRange::length).sum();
                                    logger.trace("cache file [{}] has size [{}]", getValue(document, CACHE_ID_FIELD), size);
                                    aggregateSize += size;
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (aggregateSize > 0L) {
                return aggregateSize;
            }
        }
        return 0L;
    }

    /**
     * This method repopulates the {@link CacheService} by looking at the files on the disk and for each file found, retrieves the latest
     * synchronized information and puts the cache file into the searchable snapshots cache.
     *
     * This method iterates over all node data paths and all shard directories in order to found the "snapshot_cache" directories that
     * contain the cache files. When such a directory is found, the method iterates over the cache files and looks up their name/UUID in
     * the existing Lucene documents that were loaded when instanciating the persistent cache index). If no information is found (ie no
     * matching docs in the map of Lucene documents) then the file is deleted from disk. If a doc is found the stored fields are extracted
     * from the Lucene document and are used to rebuild the necessary {@link CacheKey}, {@link SnapshotId}, {@link IndexId}, {@link ShardId}
     * and cache file ranges objects. The Lucene document is then indexed again in the new persistent cache index (the current
     * {@link CacheIndexWriter}) and the cache file is added back to the searchable snapshots cache again. Note that adding cache
     * file to the cache service might trigger evictions so previously reindexed Lucene cache files might be delete again (see
     * CacheService#onCacheFileRemoval(CacheFile) method which calls {@link #removeCacheFile(CacheFile)}.
     *
     * @param cacheService the {@link CacheService} to use when repopulating {@link CacheFile}.
     */
    void repopulateCache(CacheService cacheService) {
        ensureOpen();
        if (started.compareAndSet(false, true)) {
            try {
                for (CacheIndexWriter writer : writers) {
                    final NodeEnvironment.NodePath nodePath = writer.nodePath();
                    logger.debug("loading persistent cache on data path [{}]", nodePath);

                    for (String indexUUID : nodeEnvironment.availableIndexFoldersForPath(nodePath)) {
                        for (ShardId shardId : nodeEnvironment.findAllShardIds(new Index("_unknown_", indexUUID))) {
                            final Path shardDataPath = writer.nodePath().resolve(shardId);
                            final Path shardCachePath = getShardCachePath(new ShardPath(false, shardDataPath, shardDataPath, shardId));

                            if (Files.isDirectory(shardCachePath)) {
                                logger.trace("found snapshot cache dir at [{}], loading cache files from disk and index", shardCachePath);
                                Files.walkFileTree(shardCachePath, new SimpleFileVisitor<>() {
                                    @Override
                                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                                        try {
                                            final String id = buildId(file);
                                            final Document cacheDocument = documents.get(id);
                                            if (cacheDocument != null) {
                                                logger.trace("indexing cache file with id [{}] in persistent cache index", id);
                                                writer.updateCacheFile(id, cacheDocument);

                                                final CacheKey cacheKey = buildCacheKey(cacheDocument);
                                                final long fileLength = getFileLength(cacheDocument);
                                                final SortedSet<ByteRange> ranges = buildCacheFileRanges(cacheDocument);

                                                logger.trace("adding cache file with [id={}, key={}, ranges={}]", id, cacheKey, ranges);
                                                cacheService.put(cacheKey, fileLength, file.getParent(), id, ranges);
                                            } else {
                                                logger.trace("deleting cache file [{}] (does not exist in persistent cache index)", file);
                                                Files.delete(file);
                                            }
                                        } catch (Exception e) {
                                            throw ExceptionsHelper.convertToRuntime(e);
                                        }
                                        return FileVisitResult.CONTINUE;
                                    }
                                });
                            }
                        }
                    }
                }
                for (CacheIndexWriter writer : writers) {
                    writer.commit();
                }
                logger.info("persistent cache index loaded");
                documents.clear();
            } catch (IOException e) {
                try {
                    close();
                } catch (Exception e2) {
                    logger.warn("failed to close persistent cache index", e2);
                    e.addSuppressed(e2);
                }
                throw new UncheckedIOException("Failed to load persistent cache", e);
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
        } else {
            assert false : "persistent cache is already loaded";
        }
    }

    void commit() throws IOException {
        ensureOpen();
        try {
            for (CacheIndexWriter writer : writers) {
                writer.commit();
            }
        } catch (IOException e) {
            try {
                close();
            } catch (Exception e2) {
                logger.warn("failed to close persistent cache index writer", e2);
                e.addSuppressed(e2);
            }
            throw e;
        } finally {
            closeIfAnyIndexWriterHasTragedyOrIsClosed();
        }
    }

    private void closeIfAnyIndexWriterHasTragedyOrIsClosed() {
        if (writers.stream().map(writer -> writer.indexWriter).anyMatch(iw -> iw.getTragicException() != null || iw.isOpen() == false)) {
            try {
                close();
            } catch (Exception e) {
                logger.warn("failed to close persistent cache index", e);
            }
        }
    }

    public long getNumDocs() {
        ensureOpen();
        long count = 0L;
        for (CacheIndexWriter writer : writers) {
            count += writer.indexWriter.getPendingNumDocs();
        }
        return count;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                IOUtils.close(writers);
            } finally {
                documents.clear();
            }
        }
    }

    /**
     * Creates a list of {@link CacheIndexWriter}, one for each data path of the specified {@link NodeEnvironment}.
     *
     * @param nodeEnvironment the data node environment
     * @return a list of {@link CacheIndexWriter}
     */
    private static List<CacheIndexWriter> createWriters(NodeEnvironment nodeEnvironment) {
        final List<CacheIndexWriter> writers = new ArrayList<>();
        boolean success = false;
        try {
            final NodeEnvironment.NodePath nodePath = nodeEnvironment.nodePath();
            writers.add(createCacheIndexWriter(nodePath));
            success = true;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create persistent cache writers", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writers);
            }
        }
        return unmodifiableList(writers);
    }

    /**
     * Creates a new {@link CacheIndexWriter} for the specified data path. The is a single instance per data path.
     *
     * @param nodePath the data path
     * @return a new {@link CacheIndexWriter} instance
     * @throws IOException if something went wrong
     */
    static CacheIndexWriter createCacheIndexWriter(NodeEnvironment.NodePath nodePath) throws IOException {
        final List<Closeable> closeables = new ArrayList<>();
        boolean success = false;
        try {
            Path directoryPath = createCacheIndexFolder(nodePath);
            final Directory directory = FSDirectory.open(directoryPath);
            closeables.add(directory);

            final IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer());
            config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            config.setMergeScheduler(new SerialMergeScheduler());
            config.setRAMBufferSizeMB(1.0);
            config.setCommitOnClose(false);

            final IndexWriter indexWriter = new IndexWriter(directory, config);
            closeables.add(indexWriter);

            final CacheIndexWriter cacheIndexWriter = new CacheIndexWriter(nodePath, directory, indexWriter);
            success = true;
            return cacheIndexWriter;
        } finally {
            if (success == false) {
                IOUtils.close(closeables);
            }
        }
    }

    /**
     * Load existing documents from persistent cache indices located at the root of every node path.
     *
     * @param nodeEnvironment the data node environment
     * @return a map of {cache file uuid, Lucene document}
     */
    static Map<String, Document> loadDocuments(NodeEnvironment nodeEnvironment) {
        final Map<String, Document> documents = new HashMap<>();
        try {
            NodeEnvironment.NodePath nodePath = nodeEnvironment.nodePath();
            final Path directoryPath = resolveCacheIndexFolder(nodePath);
            if (Files.exists(directoryPath)) {
                documents.putAll(loadDocuments(directoryPath));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load existing documents from persistent cache index", e);
        }
        return documents;
    }

    /**
     * Load existing documents from a persistent cache Lucene directory.
     *
     * @param directoryPath the Lucene directory path
     * @return a map of {cache file uuid, Lucene document}
     */
    static Map<String, Document> loadDocuments(Path directoryPath) throws IOException {
        final Map<String, Document> documents = new HashMap<>();
        try (Directory directory = FSDirectory.open(directoryPath)) {
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                logger.trace("loading documents from persistent cache index [{}]", directoryPath);
                for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
                    final LeafReader leafReader = leafReaderContext.reader();
                    final Bits liveDocs = leafReader.getLiveDocs();
                    for (int i = 0; i < leafReader.maxDoc(); i++) {
                        if (liveDocs == null || liveDocs.get(i)) {
                            final Document document = leafReader.document(i);
                            logger.trace("loading document [{}]", document);
                            documents.put(getValue(document, CACHE_ID_FIELD), document);
                        }
                    }
                }
            } catch (IndexNotFoundException e) {
                logger.debug("persistent cache index does not exist yet", e);
            }
        }
        return documents;
    }

    /**
     * Cleans any leftover searchable snapshot caches (files and Lucene indices) when a non-data node is starting up.
     * This is useful when the node is repurposed and is not a data node anymore.
     *
     * @param nodeEnvironment the {@link NodeEnvironment} to cleanup
     */
    public static void cleanUp(Settings settings, NodeEnvironment nodeEnvironment) {
        final boolean isDataNode = DiscoveryNode.canContainData(settings);
        if (isDataNode) {
            assert false : "should not be called on data nodes";
            throw new IllegalStateException("Cannot clean searchable snapshot caches: node is a data node");
        }
        try {
            NodeEnvironment.NodePath nodePath = nodeEnvironment.nodePath();
            for (String indexUUID : nodeEnvironment.availableIndexFoldersForPath(nodePath)) {
                for (ShardId shardId : nodeEnvironment.findAllShardIds(new Index("_unknown_", indexUUID))) {
                    final Path shardDataPath = nodePath.resolve(shardId);
                    final ShardPath shardPath = new ShardPath(false, shardDataPath, shardDataPath, shardId);
                    final Path cacheDir = getShardCachePath(shardPath);
                    if (Files.isDirectory(cacheDir)) {
                        logger.debug("deleting searchable snapshot shard cache directory [{}]", cacheDir);
                        IOUtils.rm(cacheDir);
                    }
                }
            }
            final Path cacheIndexDir = resolveCacheIndexFolder(nodePath);
            if (Files.isDirectory(cacheIndexDir)) {
                logger.debug("deleting searchable snapshot lucene directory [{}]", cacheIndexDir);
                IOUtils.rm(cacheIndexDir);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to clean up searchable snapshots cache", e);
        }
    }

    /**
     * A {@link CacheIndexWriter} contains a Lucene {@link Directory} with an {@link IndexWriter} that can be used to index documents in
     * the persistent cache index. There is one {@link CacheIndexWriter} for each data path.
     */
    static class CacheIndexWriter implements Closeable {

        private final NodeEnvironment.NodePath nodePath;
        private final IndexWriter indexWriter;
        private final Directory directory;

        private CacheIndexWriter(NodeEnvironment.NodePath nodePath, Directory directory, IndexWriter indexWriter) {
            this.nodePath = nodePath;
            this.directory = directory;
            this.indexWriter = indexWriter;
        }

        NodeEnvironment.NodePath nodePath() {
            return nodePath;
        }

        void updateCacheFile(CacheFile cacheFile, SortedSet<ByteRange> cacheRanges) throws IOException {
            updateCacheFile(buildId(cacheFile), buildDocument(nodePath, cacheFile, cacheRanges));
        }

        void updateCacheFile(String cacheFileId, Document cacheFileDocument) throws IOException {
            final Term term = buildTerm(cacheFileId);
            logger.debug("updating document with term [{}]", term);
            indexWriter.updateDocument(term, cacheFileDocument);
        }

        void deleteCacheFile(CacheFile cacheFile) throws IOException {
            deleteCacheFile(buildId(cacheFile));
        }

        void deleteCacheFile(String cacheFileId) throws IOException {
            final Term term = buildTerm(cacheFileId);
            logger.debug("deleting document with term [{}]", term);
            indexWriter.deleteDocuments(term);
        }

        private static final Set<Map.Entry<String, String>> LUCENE_COMMIT_DATA = Collections.singletonMap(
            NODE_VERSION_COMMIT_KEY,
            Integer.toString(Version.CURRENT.id)
        ).entrySet();

        void commit() throws IOException {
            logger.debug("committing");
            indexWriter.setLiveCommitData(LUCENE_COMMIT_DATA);
            indexWriter.commit();
        }

        @Override
        public void close() throws IOException {
            logger.debug("closing persistent cache index");
            IOUtils.close(indexWriter, directory);
        }

        @Override
        public String toString() {
            return "[persistent cache index][" + nodePath + ']';
        }
    }

    private static final String CACHE_ID_FIELD = "cache_id";
    private static final String CACHE_PATH_FIELD = "cache_path";
    private static final String CACHE_RANGES_FIELD = "cache_ranges";
    private static final String SNAPSHOT_ID_FIELD = "snapshot_id";
    private static final String SNAPSHOT_INDEX_NAME_FIELD = "index_name";
    private static final String SHARD_INDEX_NAME_FIELD = "shard_index_name";
    private static final String SHARD_INDEX_ID_FIELD = "shard_index_id";
    private static final String SHARD_ID_FIELD = "shard_id";
    private static final String FILE_NAME_FIELD = "file_name";
    private static final String FILE_LENGTH_FIELD = "file_length";

    private static String buildId(CacheFile cacheFile) {
        return buildId(cacheFile.getFile());
    }

    private static String buildId(Path path) {
        return path.getFileName().toString();
    }

    private static Term buildTerm(String cacheFileUuid) {
        return new Term(CACHE_ID_FIELD, cacheFileUuid);
    }

    private static Document buildDocument(NodeEnvironment.NodePath nodePath, CacheFile cacheFile, SortedSet<ByteRange> cacheRanges)
        throws IOException {
        final Document document = new Document();
        document.add(new StringField(CACHE_ID_FIELD, buildId(cacheFile), Field.Store.YES));
        document.add(new StringField(CACHE_PATH_FIELD, nodePath.indicesPath.relativize(cacheFile.getFile()).toString(), Field.Store.YES));

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeVInt(cacheRanges.size());
            for (ByteRange cacheRange : cacheRanges) {
                output.writeVLong(cacheRange.start());
                output.writeVLong(cacheRange.end());
            }
            output.flush();
            document.add(new StoredField(CACHE_RANGES_FIELD, output.bytes().toBytesRef()));
        }

        final CacheKey cacheKey = cacheFile.getCacheKey();
        document.add(new StringField(FILE_NAME_FIELD, cacheKey.getFileName(), Field.Store.YES));
        document.add(new StringField(FILE_LENGTH_FIELD, Long.toString(cacheFile.getLength()), Field.Store.YES));
        document.add(new StringField(SNAPSHOT_ID_FIELD, cacheKey.getSnapshotUUID(), Field.Store.YES));
        document.add(new StringField(SNAPSHOT_INDEX_NAME_FIELD, cacheKey.getSnapshotIndexName(), Field.Store.YES));

        final ShardId shardId = cacheKey.getShardId();
        document.add(new StringField(SHARD_INDEX_NAME_FIELD, shardId.getIndex().getName(), Field.Store.YES));
        document.add(new StringField(SHARD_INDEX_ID_FIELD, shardId.getIndex().getUUID(), Field.Store.YES));
        document.add(new StringField(SHARD_ID_FIELD, Integer.toString(shardId.getId()), Field.Store.YES));

        return document;
    }

    private static String getValue(Document document, String fieldName) {
        final String value = document.get(fieldName);
        assert value != null : "no value found for field [" + fieldName + "] and document [" + document + ']';
        return value;
    }

    private static CacheKey buildCacheKey(Document document) {
        return new CacheKey(
            getValue(document, SNAPSHOT_ID_FIELD),
            getValue(document, SNAPSHOT_INDEX_NAME_FIELD),
            new ShardId(
                new Index(getValue(document, SHARD_INDEX_NAME_FIELD), getValue(document, SHARD_INDEX_ID_FIELD)),
                Integer.parseInt(getValue(document, SHARD_ID_FIELD))
            ),
            getValue(document, FILE_NAME_FIELD)
        );
    }

    private static long getFileLength(Document document) {
        final String fileLength = getValue(document, FILE_LENGTH_FIELD);
        assert fileLength != null;
        return Long.parseLong(fileLength);
    }

    private static SortedSet<ByteRange> buildCacheFileRanges(Document document) throws IOException {
        final BytesRef cacheRangesBytesRef = document.getBinaryValue(CACHE_RANGES_FIELD);
        assert cacheRangesBytesRef != null;

        final SortedSet<ByteRange> cacheRanges = new TreeSet<>();
        try (StreamInput input = new ByteBufferStreamInput(ByteBuffer.wrap(cacheRangesBytesRef.bytes))) {
            final int length = input.readVInt();
            assert length > 0 : "empty cache ranges";
            ByteRange previous = null;
            for (int i = 0; i < length; i++) {
                final ByteRange range = ByteRange.of(input.readVLong(), input.readVLong());
                assert range.length() > 0 : range;
                assert range.end() <= getFileLength(document);
                assert previous == null || previous.end() < range.start();

                final boolean added = cacheRanges.add(range);
                assert added : range + " already exist in " + cacheRanges;
                previous = range;
            }
        }
        return unmodifiableSortedSet(cacheRanges);
    }

    static Path resolveCacheIndexFolder(NodeEnvironment.NodePath nodePath) {
        return resolveCacheIndexFolder(nodePath.path);
    }

    static Path resolveCacheIndexFolder(Path dataPath) {
        return CacheService.resolveSnapshotCache(dataPath);
    }

    /**
     * Creates a directory for the snapshot cache Lucene index.
     */
    private static Path createCacheIndexFolder(NodeEnvironment.NodePath nodePath) throws IOException {
        // "snapshot_cache" directory at the root of the specified data path
        final Path snapshotCacheRootDir = resolveCacheIndexFolder(nodePath);
        if (Files.exists(snapshotCacheRootDir) == false) {
            logger.debug("creating new persistent cache index directory [{}]", snapshotCacheRootDir);
            Files.createDirectories(snapshotCacheRootDir);
        }
        return snapshotCacheRootDir;
    }

    /**
     * Exception thrown when the {@link CacheIndexWriter} corresponding to a given {@link CacheFile} cannot be found.
     */
    static class PersistentCacheIndexNotFoundException extends IllegalArgumentException {

        PersistentCacheIndexNotFoundException(NodeEnvironment nodeEnvironment, CacheFile cacheFile) {
            super(
                "Persistent cache index not found for cache file path ["
                    + cacheFile.getFile()
                    + "] using node path "
                    + nodeEnvironment.nodeDataPath()
            );
        }
    }
}
