/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.iceberg.DataFile;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xpack.parquet.write.file.IndexInputFile;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.ParquetMerger;
import org.elasticsearch.xpack.parquet.write.file.TestLocalFiles;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/** Every commit of a table creates or updates its entry in the catalog, and a failure of the catalog never fails a commit. */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        CatalogCommitTests.IcebergWorkerPoolFilter.class }
)
public class CatalogCommitTests extends ESTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final String BASE_URI = "file:///warehouse";
    private static final TableLocation LOCATION = new TableLocation(BASE_URI + "/logs", BlobPath.EMPTY.add("logs"));
    private static final List<String> NAMESPACE = List.of("default");

    private Path root;
    private FsBlobStore store;
    private Directory directory;
    private RepositoryCatalog catalog;

    @Before
    public void createStores() throws Exception {
        root = createTempDir();
        store = new FsBlobStore(4096, root, false);
        directory = new ByteBuffersDirectory();
        catalog = new RepositoryCatalog(store, BlobPath.EMPTY, BASE_URI);
    }

    @After
    public void closeStores() throws Exception {
        store.close();
        directory.close();
    }

    private ShardTable open(int shard, boolean withCatalog) {
        final CatalogRegistration registration = withCatalog
            ? new CatalogRegistration(catalog, NAMESPACE, "logs", "logs", "index-uuid", "cluster-uuid")
            : null;
        return TestTables.open(store, LOCATION, shard, registration);
    }

    private void append(ShardTable table, long from, long to) throws IOException {
        final LocalParquetFiles.LocalFile local = TestLocalFiles.write(directory, from, to, false);
        final MessageType union = ParquetMerger.unionSchema(directory, List.of(local));
        final MessageType withIds = ShardTable.withFieldIds(union, table.ensureColumns(union));
        final String name = LocalParquetFiles.mergedName(from, to);
        final ParquetMerger.Merged merged = ParquetMerger.merge(
            directory,
            List.of(local),
            withIds,
            name,
            CompressionCodecName.SNAPPY,
            1 << 20
        );
        final String attempt = ShardTable.newAttempt();
        try (IndexInput in = directory.openInput(merged.name(), IOContext.DEFAULT)) {
            table.upload(
                from,
                to,
                attempt,
                new java.io.BufferedInputStream(new InputStreamIndexInput(in, merged.length())),
                merged.length()
            );
        }
        final DataFile file = table.dataFile(from, to, attempt, merged.length(), new IndexInputFile(directory, merged.name()));
        table.append(file, to);
    }

    private int newestVersionOnDisk() throws IOException {
        int newest = 0;
        for (String name : store.blobContainer(BlobPath.EMPTY.add("logs").add("metadata")).listBlobs(RepositoryFileIO.PURPOSE).keySet()) {
            final var matcher = java.util.regex.Pattern.compile("v(\\d+)\\.metadata\\.json").matcher(name);
            if (matcher.matches()) {
                newest = Math.max(newest, Integer.parseInt(matcher.group(1)));
            }
        }
        return newest;
    }

    public void testCreatingTheTableCreatesItsEntry() throws Exception {
        open(0, true);

        final CatalogEntry entry = catalog.read(NAMESPACE, "logs").orElseThrow();
        assertThat(entry.metadataVersion(), equalTo(1));
        assertThat(entry.location(), equalTo(LOCATION.rootUri()));
        assertThat(entry.metadataLocation(), equalTo(LOCATION.rootUri() + "/metadata/v1.metadata.json"));
        assertThat(entry.index(), equalTo("logs"));
        assertThat(entry.indexUuid(), equalTo("index-uuid"));
        assertThat(entry.clusterUuid(), equalTo("cluster-uuid"));
        assertTrue("the table uuid is the one of the Iceberg metadata", entry.tableUuid().length() > 10);
    }

    public void testEveryCommitUpdatesTheEntryToTheNewestMetadataFile() throws Exception {
        final ShardTable table = open(0, true);
        final CatalogEntry created = catalog.read(NAMESPACE, "logs").orElseThrow();

        append(table, 0, 99); // a schema commit and an append
        final CatalogEntry afterFirst = catalog.read(NAMESPACE, "logs").orElseThrow();
        append(table, 100, 199);
        final CatalogEntry afterSecond = catalog.read(NAMESPACE, "logs").orElseThrow();

        assertThat(afterFirst.metadataVersion(), equalTo(newestVersionOnDisk() - 1));
        assertThat(afterSecond.metadataVersion(), equalTo(newestVersionOnDisk()));
        assertThat(afterSecond.metadataVersion(), greaterThan(afterFirst.metadataVersion()));
        assertThat(afterSecond.previousMetadataLocation(), equalTo(afterFirst.metadataLocation()));
        assertThat(
            "one table: the creation time and the uuid do not change",
            afterSecond.createdAtMillis(),
            equalTo(created.createdAtMillis())
        );
        assertThat(afterSecond.tableUuid(), equalTo(created.tableUuid()));
        // the file that the entry names is there
        assertTrue(
            store.blobContainer(BlobPath.EMPTY.add("logs").add("metadata"))
                .blobExists(RepositoryFileIO.PURPOSE, "v" + afterSecond.metadataVersion() + ".metadata.json")
        );
        assertThat(catalog.list(NAMESPACE), equalTo(List.of("logs")));
    }

    public void testTheShardsOfOneIndexShareOneEntryThatLoadResolvesToTheNewestTable() throws Exception {
        final ShardTable shard0 = open(0, true);
        final ShardTable shard1 = open(1, true);

        append(shard0, 0, 99);
        append(shard1, 1_000, 1_099); // the local files of the two shards share the directory of this test, so the ranges differ
        append(shard0, 100, 199);

        assertThat(catalog.list(NAMESPACE), equalTo(List.of("logs")));
        assertThat(catalog.load(NAMESPACE, "logs").orElseThrow().metadataVersion(), equalTo(newestVersionOnDisk()));
    }

    public void testATableWithoutACatalogWritesNoEntry() throws Exception {
        final ShardTable table = open(0, false);
        append(table, 0, 99);

        // the file system store creates a directory when it makes a container, so look at the disk first
        assertFalse("no directory of the catalog", Files.exists(root.resolve(RepositoryCatalog.DIRECTORY)));
        assertThat(catalog.read(NAMESPACE, "logs"), equalTo(Optional.empty()));
    }

    public void testAFailureOfTheCatalogDoesNotFailTheCommitsOfTheTable() throws Exception {
        // a file where the directory of the catalog should be: every write of an entry fails
        Files.write(root.resolve(RepositoryCatalog.DIRECTORY), new byte[] { 1 });
        final ShardTable table = open(0, true);

        append(table, 0, 99);
        append(table, 100, 199);

        assertThat("the table is committed and readable", table.uploadedFiles(), hasSize(2));
        assertThat(newestVersionOnDisk(), greaterThan(1));
        assertTrue(Files.isRegularFile(root.resolve(RepositoryCatalog.DIRECTORY)));
        final ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> catalog.read(NAMESPACE, "logs"));
        assertThat(e.getMessage(), containsString("failed to create blob container"));
    }
}
