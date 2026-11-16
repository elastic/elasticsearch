/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.Directory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.parquet.write.file.TestParquetBytes;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Spike S2: a flush merges the local files and uploads them to a real {@code fs} repository as the files of an Iceberg table, and
 * a restart finds what is uploaded and what is not.
 */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetTypesIT.IcebergWorkerPoolFilter.class }
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ParquetTypesIT extends ESIntegTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements com.carrotsearch.randomizedtesting.ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final String INDEX = "parquet-types";
    private static final String REPOSITORY = "lake";
    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    private static Path repoRoot;

    @BeforeClass
    public static void createRepoRoot() {
        repoRoot = createTempDir();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Environment.PATH_REPO_SETTING.getKey(), repoRoot)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ParquetWriteEnginePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected void beforeIndexDeletion() {
        // the base class searches every shard, and this engine cannot be searched
    }

    private void registerRepository() {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoRoot.resolve(REPOSITORY).toString()))
        );
    }

    private void createIndex(int shards, Settings.Builder extra) throws IOException {
        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("host.name")
            .field("type", "keyword")
            .endObject()
            .startObject("count")
            .field("type", "long")
            .endObject()
            .startObject("bytes")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", shards)
                        .put("index.parquet.repository", REPOSITORY)
                        .put("index.parquet.local_compression", randomFrom("uncompressed", "snappy", "zstd"))
                        .put(extra.build())
                )
                .setMapping(mapping)
        );
        ensureGreen(INDEX);
    }

    private BulkResponse bulk(int from, int to) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = from; i < to; i++) {
            bulk.add(
                new IndexRequest(INDEX).source(
                    "@timestamp",
                    BASE_TIMESTAMP + i * 1000L,
                    "host.name",
                    "host-" + (i % 7),
                    "count",
                    i,
                    "bytes",
                    i % 1000
                )
            );
        }
        return bulk.get();
    }

    private IndexShard shard(int shard) {
        return internalCluster().getDataNodeInstance(IndicesService.class).indexServiceSafe(resolveIndex(INDEX)).getShard(shard);
    }

    private long localFiles(int shard) throws IOException {
        final Directory directory = shard(shard).store().directory();
        long files = 0;
        for (String name : directory.listAll()) {
            if (name.startsWith("pqlocal_") || name.startsWith("pqmerged_")) {
                files++;
            }
        }
        return files;
    }

    /** The table of the index, as a reader outside the engine opens it: from the repository directory. */
    private ShardTable table(int shard) throws IOException {
        final String root = repoRoot.resolve(REPOSITORY).toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
        final String tablePath = INDEX + "/" + resolveIndex(INDEX).getUUID();
        final FsBlobStore store = new FsBlobStore(4096, repoRoot.resolve(REPOSITORY), true);
        return ShardTable.open(
            store,
            new TableLocation(root + "/" + tablePath, BlobPath.EMPTY.add(INDEX).add(resolveIndex(INDEX).getUUID())),
            shard
        );
    }

    /** The values of {@code count} in the files that the table lists for the shards, each once. */
    private TreeSet<Long> uploadedCounts(int shards) throws IOException {
        final TreeSet<Long> counts = new TreeSet<>();
        final String root = repoRoot.resolve(REPOSITORY).toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
        for (int shard = 0; shard < shards; shard++) {
            long expectedFrom = 0;
            for (ShardTable.UploadedFile file : table(shard).uploadedFiles()) {
                assertThat("the files of shard " + shard + " are contiguous", file.fromSeqNo(), equalTo(expectedFrom));
                expectedFrom = file.toSeqNo() + 1;
                final Path path = repoRoot.resolve(REPOSITORY)
                    .resolve(INDEX)
                    .resolve(resolveIndex(INDEX).getUUID())
                    .resolve("data")
                    .resolve("shard-" + shard)
                    .resolve(ShardTable.dataFileName(file.fromSeqNo(), file.toSeqNo()));
                final TestParquetBytes.Contents contents = TestParquetBytes.read(java.nio.file.Files.readAllBytes(path));
                assertThat(contents.rows().size(), equalTo((int) file.rows()));
                for (var row : contents.rows()) {
                    assertTrue("count appears once", counts.add(row.getLong("count", 0)));
                }
            }
        }
        return counts;
    }

    private void assertUploaded(int shards, int rows) throws IOException {
        final TreeSet<Long> counts = uploadedCounts(shards);
        assertThat(counts.size(), equalTo(rows));
        assertThat(counts.first(), equalTo(0L));
        assertThat(counts.last(), equalTo(rows - 1L));
    }

    private static final int DOCS = 3_000;

    /** The value of each field of document {@code n}, or null for a document that lacks the field. */
    private static Long longOf(int n) {
        return n % 7 == 0 ? null : n * 1_000_003L - 5_000_000_000L;
    }

    private static Integer intOf(int n) {
        return n - 1_500;
    }

    private static Integer shortOf(int n) {
        return (n * 7) % 60_000 - 30_000;
    }

    private static Integer byteOf(int n) {
        return n % 256 - 128;
    }

    private static Double doubleOf(int n) {
        if (n % 9 == 0) {
            return null;
        }
        return n % 11 == 0 ? Double.MAX_VALUE : n * 0.25 - 100.125;
    }

    private static Float floatOf(int n) {
        return n % 13 == 0 ? Float.MIN_VALUE : n * 0.5f - 7.25f;
    }

    private static Boolean booleanOf(int n) {
        return n % 11 == 3 ? null : n % 3 == 0;
    }

    private static String keywordOf(int n) {
        return n % 5 == 0 ? null : "kw-" + (n % 13);
    }

    private void createTypesIndex(Settings.Builder extra) throws IOException {
        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("n")
            .field("type", "integer")
            .endObject()
            .startObject("ts")
            .field("type", "date")
            .endObject()
            .startObject("k")
            .field("type", "keyword")
            .endObject()
            .startObject("l")
            .field("type", "long")
            .endObject()
            .startObject("i")
            .field("type", "integer")
            .endObject()
            .startObject("s")
            .field("type", "short")
            .endObject()
            .startObject("b")
            .field("type", "byte")
            .endObject()
            .startObject("d")
            .field("type", "double")
            .endObject()
            .startObject("f")
            .field("type", "float")
            .endObject()
            .startObject("flag")
            .field("type", "boolean")
            .endObject()
            .startObject("o")
            .startObject("properties")
            .startObject("inner")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", 1)
                        .put("index.parquet.repository", REPOSITORY)
                        .put(extra.build())
                )
                .setMapping(mapping)
        );
        ensureGreen(INDEX);
    }

    private BulkResponse bulkTypes(int from, int to) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int n = from; n < to; n++) {
            final Map<String, Object> source = new LinkedHashMap<>();
            source.put("n", n);
            source.put("ts", BASE_TIMESTAMP + n * 1000L);
            putIfPresent(source, "k", keywordOf(n));
            putIfPresent(source, "l", longOf(n));
            source.put("i", intOf(n));
            source.put("s", shortOf(n));
            source.put("b", byteOf(n));
            putIfPresent(source, "d", doubleOf(n));
            source.put("f", floatOf(n));
            putIfPresent(source, "flag", booleanOf(n));
            source.put("o.inner", (long) n * 2);
            bulk.add(new IndexRequest(INDEX).source(source));
        }
        return bulk.get();
    }

    private static void putIfPresent(Map<String, Object> source, String field, Object value) {
        if (value != null) {
            source.put(field, value);
        }
    }

    /** The rows of the files that the table lists for the shard, in the order of the files. */
    private List<Group> uploadedRows() throws IOException {
        final List<Group> rows = new ArrayList<>();
        for (TestParquetBytes.Contents contents : uploadedContents()) {
            rows.addAll(contents.rows());
        }
        return rows;
    }

    private List<TestParquetBytes.Contents> uploadedContents() throws IOException {
        final List<TestParquetBytes.Contents> all = new ArrayList<>();
        for (ShardTable.UploadedFile file : table(0).uploadedFiles()) {
            final Path path = repoRoot.resolve(REPOSITORY)
                .resolve(INDEX)
                .resolve(resolveIndex(INDEX).getUUID())
                .resolve("data")
                .resolve("shard-0")
                .resolve(ShardTable.dataFileName(file.fromSeqNo(), file.toSeqNo()));
            all.add(TestParquetBytes.read(java.nio.file.Files.readAllBytes(path)));
        }
        return all;
    }

    private static <T> void assertField(Group row, String field, T expected, java.util.function.BiFunction<Group, String, T> read) {
        final int n = row.getInteger("n", 0);
        if (expected == null) {
            assertThat("document " + n + " has no [" + field + "]", row.getFieldRepetitionCount(field), equalTo(0));
        } else {
            assertThat("document " + n + " [" + field + "]", read.apply(row, field), equalTo(expected));
        }
    }

    public void testEverySupportedTypeIsWrittenAndReadBackWithItsValue() throws Exception {
        registerRepository();
        createTypesIndex(Settings.builder().put("index.parquet.compaction.enabled", false));

        assertFalse(bulkTypes(0, DOCS).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        final List<TestParquetBytes.Contents> contents = uploadedContents();
        assertThat(contents, hasSize(1));
        // the physical and the logical types of the columns
        final var schema = contents.get(0).schema();
        assertThat(schema.getType("d").asPrimitiveType().getPrimitiveTypeName(), equalTo(PrimitiveType.PrimitiveTypeName.DOUBLE));
        assertThat(schema.getType("f").asPrimitiveType().getPrimitiveTypeName(), equalTo(PrimitiveType.PrimitiveTypeName.FLOAT));
        assertThat(schema.getType("flag").asPrimitiveType().getPrimitiveTypeName(), equalTo(PrimitiveType.PrimitiveTypeName.BOOLEAN));
        assertThat(schema.getType("s").getLogicalTypeAnnotation(), equalTo(LogicalTypeAnnotation.intType(16, true)));
        assertThat(schema.getType("b").getLogicalTypeAnnotation(), equalTo(LogicalTypeAnnotation.intType(8, true)));
        assertThat(
            schema.getType("ts").getLogicalTypeAnnotation(),
            equalTo(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
        );
        assertTrue("a dotted name is one flat column", schema.containsField("o.inner"));

        // every value of every document
        final List<Group> rows = uploadedRows();
        assertThat(rows, hasSize(DOCS));
        final TreeSet<Integer> seen = new TreeSet<>();
        for (Group row : rows) {
            final int n = row.getInteger("n", 0);
            assertTrue("each document once", seen.add(n));
            assertField(row, "ts", BASE_TIMESTAMP + n * 1000L, (g, f) -> g.getLong(f, 0));
            assertField(row, "k", keywordOf(n), (g, f) -> g.getString(f, 0));
            assertField(row, "l", longOf(n), (g, f) -> g.getLong(f, 0));
            assertField(row, "i", intOf(n), (g, f) -> g.getInteger(f, 0));
            assertField(row, "s", shortOf(n), (g, f) -> g.getInteger(f, 0));
            assertField(row, "b", byteOf(n), (g, f) -> g.getInteger(f, 0));
            assertField(row, "d", doubleOf(n), (g, f) -> g.getDouble(f, 0));
            assertField(row, "f", floatOf(n), (g, f) -> g.getFloat(f, 0));
            assertField(row, "flag", booleanOf(n), (g, f) -> g.getBoolean(f, 0));
            assertField(row, "o.inner", (long) n * 2, (g, f) -> g.getLong(f, 0));
        }
        assertThat(seen.size(), equalTo(DOCS));

        // the table has the Iceberg type of each column
        final var columns = table(0).schema();
        assertThat(columns.findType("d"), equalTo(org.apache.iceberg.types.Types.DoubleType.get()));
        assertThat(columns.findType("f"), equalTo(org.apache.iceberg.types.Types.FloatType.get()));
        assertThat(columns.findType("flag"), equalTo(org.apache.iceberg.types.Types.BooleanType.get()));
        assertThat(columns.findType("s"), equalTo(org.apache.iceberg.types.Types.IntegerType.get()));
        assertThat(columns.findType("b"), equalTo(org.apache.iceberg.types.Types.IntegerType.get()));
        assertThat(columns.findType("ts"), equalTo(org.apache.iceberg.types.Types.TimestampType.withZone()));
    }

    public void testAFieldAddedByAMappingUpdateIsNullInTheEarlierRowsAndSurvivesACompaction() throws Exception {
        registerRepository();
        createTypesIndex(Settings.builder().put("index.parquet.compaction.enabled", false));
        assertFalse(bulkTypes(0, 500).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        assertAcked(indicesAdmin().preparePutMapping(INDEX).setSource("extra", "type=long"));
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int n = 500; n < 1_000; n++) {
            bulk.add(
                new IndexRequest(INDEX).source(
                    "n",
                    n,
                    "ts",
                    BASE_TIMESTAMP + n * 1000L,
                    "i",
                    intOf(n),
                    "s",
                    shortOf(n),
                    "b",
                    byteOf(n),
                    "f",
                    floatOf(n),
                    "o.inner",
                    (long) n * 2,
                    "extra",
                    n * 10L
                )
            );
        }
        assertFalse(bulk.get().hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        // two files: the first has no [extra], the second has it, with the next id of the table
        final List<TestParquetBytes.Contents> before = uploadedContents();
        assertThat(before, hasSize(2));
        assertFalse(before.get(0).schema().containsField("extra"));
        assertTrue(before.get(1).schema().containsField("extra"));
        final int extraId = table(0).schema().findField("extra").fieldId();
        assertThat(extraId, equalTo(table(0).schema().highestFieldId()));

        // a compaction merges them: the union has the column, and the earlier rows have a null
        updateIndexSettings(Settings.builder().put("index.parquet.compaction.enabled", true), INDEX);
        final BulkRequestBuilder more = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int n = 1_000; n < 1_100; n++) {
            more.add(
                new IndexRequest(INDEX).source(
                    "n",
                    n,
                    "ts",
                    BASE_TIMESTAMP + n * 1000L,
                    "i",
                    intOf(n),
                    "s",
                    shortOf(n),
                    "b",
                    byteOf(n),
                    "f",
                    floatOf(n),
                    "o.inner",
                    (long) n * 2,
                    "extra",
                    n * 10L
                )
            );
        }
        assertFalse(more.get().hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertBusy(() -> assertThat(table(0).uploadedFiles(), hasSize(1)), 30, TimeUnit.SECONDS);

        final List<Group> rows = uploadedRows();
        assertThat(rows, hasSize(1_100));
        for (Group row : rows) {
            final int n = row.getInteger("n", 0);
            assertField(row, "extra", n < 500 ? null : n * 10L, (g, f) -> g.getLong(f, 0));
            assertField(row, "i", intOf(n), (g, f) -> g.getInteger(f, 0));
        }
        assertThat(table(0).schema().findField("extra").fieldId(), equalTo(extraId));
    }

    /**
     * A reader of the table (the reader prototype) needs every file to have every column of the others, so the schema of a file comes
     * from the mapping and not from the fields that the documents of its bulk happened to have. The first bulk has only two fields.
     */
    public void testEveryFileHasAColumnForEveryMappedFieldWhateverTheDocumentsHave() throws Exception {
        registerRepository();
        createTypesIndex(Settings.builder().put("index.parquet.compaction.enabled", false));

        final BulkRequestBuilder first = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int n = 0; n < 100; n++) {
            first.add(new IndexRequest(INDEX).source("n", n, "ts", BASE_TIMESTAMP + n * 1000L));
        }
        assertFalse(first.get().hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertFalse(bulkTypes(100, 300).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        final List<TestParquetBytes.Contents> files = uploadedContents();
        assertThat(files, hasSize(2));
        final List<String> expected = List.of("n", "ts", "k", "l", "i", "s", "b", "d", "f", "flag", "o.inner");
        for (TestParquetBytes.Contents file : files) {
            for (String field : expected) {
                assertTrue("every file has a column for [" + field + "]", file.schema().containsField(field));
            }
            assertThat(file.schema().getFieldCount(), equalTo(expected.size()));
        }
        // the types are the same in both files, and the rows of the first file are null where the documents had no value
        for (String field : expected) {
            assertThat(files.get(0).schema().getType(field), equalTo(files.get(1).schema().getType(field)));
        }
        for (Group row : files.get(0).rows()) {
            assertThat(row.getFieldRepetitionCount("l"), equalTo(0));
            assertThat(row.getFieldRepetitionCount("flag"), equalTo(0));
            assertThat(row.getFieldRepetitionCount("o.inner"), equalTo(0));
        }
        assertThat(files.get(0).rows(), hasSize(100));
    }
}
