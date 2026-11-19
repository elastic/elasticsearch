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
import java.util.List;
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
        ParquetListsIT.IcebergWorkerPoolFilter.class }
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ParquetListsIT extends ESIntegTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements com.carrotsearch.randomizedtesting.ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final String INDEX = "parquet-lists";
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
                        .put("index.parquet.durability", randomFrom("local", "staged"))
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
            shard,
            INDEX,
            resolveIndex(INDEX).getUUID(),
            shard(shard).getHistoryUUID(),
            null
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
                final Path path = org.elasticsearch.core.PathUtils.get(java.net.URI.create(file.location()));
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

    // ---- what every document has, by its number; null means that the document has no value for the field ----

    /** Keywords: missing, explicit null, empty (these three are a null list), one value, duplicates out of order, a null element. */
    private static List<String> ksOf(int n) {
        return switch (n % 7) {
            case 0, 1, 2 -> null;
            case 3 -> List.of("only-" + n);
            case 4 -> List.of("b", "a", "b", "c-" + n);
            case 5 -> java.util.Arrays.asList("x", null, "y");
            case 6 -> List.of("z");
            default -> throw new AssertionError();
        };
    }

    /** The values of every numeric field of the document, before the conversion to the type of the field. */
    private static List<Long> baseOf(int n) {
        return switch (n % 5) {
            case 0, 1 -> null; // missing, or an empty array
            case 2 -> List.of((long) n);
            case 3 -> List.of(3L * n, (long) n, 3L * n, -7L); // not sorted, and with a duplicate
            case 4 -> List.of(n * 10L);
            default -> throw new AssertionError();
        };
    }

    private static <T> List<T> convert(List<Long> base, java.util.function.Function<Long, T> convert) {
        return base == null ? null : base.stream().map(convert).toList();
    }

    private static final java.util.Map<String, java.util.function.Function<Long, ?>> NUMERIC = java.util.Map.of(
        "ls",
        v -> v,
        "is",
        v -> (int) (v % 1_000),
        "ss",
        v -> (int) (v % 30_000),
        "bs",
        v -> (int) (v % 100),
        "ds",
        v -> v * 0.25,
        "fs",
        v -> v * 0.5f,
        "o.xs",
        v -> v + 1
    );

    private static final List<String> NUMERIC_FIELDS = List.of("ls", "is", "ss", "bs", "ds", "fs", "o.xs");

    private void createListsIndex(Settings.Builder extra) throws IOException {
        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("n")
            .field("type", "integer")
            .endObject()
            .startObject("k")
            .field("type", "keyword")
            .endObject()
            .startObject("ks")
            .field("type", "keyword")
            .field("multi_value", true)
            .endObject()
            .startObject("ls")
            .field("type", "long")
            .field("multi_value", true)
            .endObject()
            .startObject("is")
            .field("type", "integer")
            .field("multi_value", true)
            .endObject()
            .startObject("ss")
            .field("type", "short")
            .field("multi_value", true)
            .endObject()
            .startObject("bs")
            .field("type", "byte")
            .field("multi_value", true)
            .endObject()
            .startObject("ds")
            .field("type", "double")
            .field("multi_value", true)
            .endObject()
            .startObject("fs")
            .field("type", "float")
            .field("multi_value", true)
            .endObject()
            .startObject("o")
            .startObject("properties")
            .startObject("xs")
            .field("type", "long")
            .field("multi_value", true)
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
                        .put("index.parquet.durability", randomFrom("local", "staged"))
                        .put("index.parquet.local_compression", randomFrom("uncompressed", "snappy", "zstd"))
                        .put(extra.build())
                )
                .setMapping(mapping)
        );
        ensureGreen(INDEX);
    }

    private static String json(Object value) {
        if (value instanceof List<?> list) {
            return list.stream().map(ParquetListsIT::json).collect(java.util.stream.Collectors.joining(",", "[", "]"));
        }
        if (value == null) {
            return "null";
        }
        return value instanceof String ? "\"" + value + "\"" : value.toString();
    }

    /** The source of document {@code n}: a scalar where the document has exactly one value and the number says so, else an array. */
    private static String sourceOf(int n) {
        final StringBuilder source = new StringBuilder("{\"n\":").append(n).append(",\"k\":\"flat-").append(n % 3).append("\"");
        switch (n % 7) {
            case 0 -> {
                /* missing */ }
            case 1 -> source.append(",\"ks\":null");
            case 2 -> source.append(",\"ks\":[]");
            case 3 -> source.append(",\"ks\":").append(n % 14 == 3 ? json(ksOf(n).get(0)) : json(ksOf(n)));
            default -> source.append(",\"ks\":").append(json(ksOf(n)));
        }
        final List<Long> base = baseOf(n);
        for (String field : NUMERIC_FIELDS) {
            if (n % 5 == 0) {
                continue; // missing
            }
            final Object value = base == null ? List.of() : convert(base, v -> NUMERIC.get(field).apply(v));
            final String name = field.equals("o.xs") ? "\"o\":{\"xs\":" : "\"" + field + "\":";
            // Always an array for a numeric list. A scalar for one document and an array for another in the same bulk takes the whole
            // bulk off the batch path (found by probing it), so that mix is a limit of the engine and not something this test does.
            final boolean scalar = false;
            source.append(',').append(name).append(scalar ? json(((List<?>) value).get(0)) : json(value));
            if (field.equals("o.xs")) {
                source.append('}');
            }
        }
        return source.append('}').toString();
    }

    private BulkResponse bulkLists(int from, int to) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int n = from; n < to; n++) {
            bulk.add(new IndexRequest(INDEX).source(sourceOf(n), org.elasticsearch.xcontent.XContentType.JSON));
        }
        return bulk.get();
    }

    private List<TestParquetBytes.Contents> uploadedContents() throws IOException {
        final List<TestParquetBytes.Contents> all = new ArrayList<>();
        for (ShardTable.UploadedFile file : table(0).uploadedFiles()) {
            final Path path = org.elasticsearch.core.PathUtils.get(java.net.URI.create(file.location()));
            all.add(TestParquetBytes.read(java.nio.file.Files.readAllBytes(path)));
        }
        return all;
    }

    /** The elements of a list column of a row, null for a null list, and null for a null element. */
    private static List<Object> listOf(Group row, String field) {
        if (row.getFieldRepetitionCount(field) == 0) {
            return null;
        }
        final Group list = row.getGroup(field, 0);
        final PrimitiveType element = list.getType().getType(0).asGroupType().getType(0).asPrimitiveType();
        final List<Object> values = new ArrayList<>();
        for (int i = 0; i < list.getFieldRepetitionCount("list"); i++) {
            final Group item = list.getGroup("list", i);
            if (item.getFieldRepetitionCount("element") == 0) {
                values.add(null);
                continue;
            }
            values.add(switch (element.getPrimitiveTypeName()) {
                case BINARY -> item.getString("element", 0);
                case INT64 -> item.getLong("element", 0);
                case INT32 -> item.getInteger("element", 0);
                case DOUBLE -> item.getDouble("element", 0);
                case FLOAT -> item.getFloat("element", 0);
                default -> throw new AssertionError(element.getPrimitiveTypeName());
            });
        }
        return values;
    }

    private void assertRowsHaveTheListsOfTheSource(List<Group> rows, int expectedRows) {
        assertThat(rows, hasSize(expectedRows));
        final TreeSet<Integer> seen = new TreeSet<>();
        for (Group row : rows) {
            final int n = row.getInteger("n", 0);
            assertTrue("each document once", seen.add(n));
            assertThat("ks of " + n, listOf(row, "ks"), equalTo(ksOf(n) == null ? null : new ArrayList<Object>(ksOf(n))));
            final List<Long> base = baseOf(n);
            for (String field : NUMERIC_FIELDS) {
                final List<?> expected = base == null ? null : convert(base, v -> NUMERIC.get(field).apply(v));
                assertThat(field + " of " + n, listOf(row, field), equalTo(expected == null ? null : new ArrayList<Object>(expected)));
            }
            assertThat("the flat keyword is not a list", row.getString("k", 0), equalTo("flat-" + n % 3));
        }
        assertThat(seen.size(), equalTo(expectedRows));
    }

    public void testEveryMultiValuedFieldIsAListWithTheValuesOfTheSource() throws Exception {
        registerRepository();
        createListsIndex(Settings.builder().put("index.parquet.compaction.enabled", false));

        final BulkResponse response = bulkLists(0, DOCS);
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        final List<TestParquetBytes.Contents> files = uploadedContents();
        assertThat(files, hasSize(1));
        // the shape of the columns: LIST groups with an optional element of the type of the field, and a flat keyword next to them
        final var schema = files.get(0).schema();
        for (String field : List.of("ks", "ls", "is", "ss", "bs", "ds", "fs", "o.xs")) {
            assertTrue("[" + field + "] is a list", org.elasticsearch.xpack.parquet.write.file.ParquetLists.isList(schema.getType(field)));
        }
        assertFalse(org.elasticsearch.xpack.parquet.write.file.ParquetLists.isList(schema.getType("k")));
        assertThat(
            org.elasticsearch.xpack.parquet.write.file.ParquetLists.elementOf(schema.getType("ss")).getLogicalTypeAnnotation(),
            equalTo(LogicalTypeAnnotation.intType(16, true))
        );
        assertThat(
            org.elasticsearch.xpack.parquet.write.file.ParquetLists.elementOf(schema.getType("ks")).getLogicalTypeAnnotation(),
            equalTo(LogicalTypeAnnotation.stringType())
        );
        // every value of every list
        assertRowsHaveTheListsOfTheSource(files.get(0).rows(), DOCS);

        // the table has a list of the type, with ids for the list and for its element
        final var columns = table(0).schema();
        for (String field : List.of("ks", "ls", "is", "ss", "bs", "ds", "fs", "o.xs")) {
            assertTrue("[" + field + "] is a list in the table", columns.findField(field).type().isListType());
        }
        assertThat(columns.findField("ks").type().asListType().elementType(), equalTo(org.apache.iceberg.types.Types.StringType.get()));
        assertThat(columns.findField("ds").type().asListType().elementType(), equalTo(org.apache.iceberg.types.Types.DoubleType.get()));
        assertThat(columns.findField("k").type(), equalTo(org.apache.iceberg.types.Types.StringType.get()));
    }

    public void testListsSurviveTwoFlushesAndTheCompactionThatMergesThem() throws Exception {
        registerRepository();
        createListsIndex(Settings.builder().put("index.parquet.compaction.enabled", false).put("index.parquet.compaction.interval", "1s"));
        assertFalse(bulkLists(0, 1_500).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertFalse(bulkLists(1_500, DOCS).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertThat(table(0).uploadedFiles(), hasSize(2));

        updateIndexSettings(Settings.builder().put("index.parquet.compaction.enabled", true), INDEX);

        assertBusy(() -> assertThat(table(0).uploadedFiles(), hasSize(1)), 30, TimeUnit.SECONDS);
        final List<TestParquetBytes.Contents> files = uploadedContents();
        assertThat(files, hasSize(1));
        assertRowsHaveTheListsOfTheSource(files.get(0).rows(), DOCS);
    }

    public void testEveryFileHasAListColumnForEveryMultiValuedFieldWhateverTheDocumentsHave() throws Exception {
        registerRepository();
        createListsIndex(Settings.builder().put("index.parquet.compaction.enabled", false));
        final BulkRequestBuilder first = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int n = 0; n < 100; n++) {
            first.add(new IndexRequest(INDEX).source("n", n, "k", "flat"));
        }
        assertFalse(first.get().hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertFalse(bulkLists(100, 600).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        final List<TestParquetBytes.Contents> files = uploadedContents();
        assertThat(files, hasSize(2));
        for (String field : List.of("ks", "ls", "is", "ss", "bs", "ds", "fs", "o.xs")) {
            assertThat(
                "the type of [" + field + "] is the same in both files",
                files.get(0).schema().getType(field),
                equalTo(files.get(1).schema().getType(field))
            );
            assertTrue(org.elasticsearch.xpack.parquet.write.file.ParquetLists.isList(files.get(0).schema().getType(field)));
        }
        for (Group row : files.get(0).rows()) {
            assertNull(listOf(row, "ks"));
            assertNull(listOf(row, "ls"));
        }
    }

    public void testASingleValuedFieldRefusesAnArrayAndAListAcceptsOneValue() throws Exception {
        registerRepository();
        createListsIndex(Settings.builder().put("index.parquet.compaction.enabled", false));

        // "k" is flat, so an array is refused for that document. It also takes the whole shard bulk off the batch path (a limit of the
        // engine: a document that the batch cannot take fails every document of its shard bulk).
        final BulkResponse refused = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
            .add(new IndexRequest(INDEX).source("{\"n\":1,\"k\":[\"a\",\"b\"]}", org.elasticsearch.xcontent.XContentType.JSON))
            .get();
        assertTrue(refused.hasFailures());
        assertThat(refused.getItems()[0].getFailureMessage(), org.hamcrest.Matchers.containsString("failed to parse field [k]"));

        // a list field with one value, as a scalar, is a list of one element
        final BulkResponse accepted = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
            .add(new IndexRequest(INDEX).source("{\"n\":2,\"k\":\"fine\",\"ks\":\"one\"}", org.elasticsearch.xcontent.XContentType.JSON))
            .get();
        assertFalse(accepted.buildFailureMessage(), accepted.hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        final List<Group> rows = uploadedContents().get(0).rows();
        assertThat(rows, hasSize(1));
        assertThat(listOf(rows.get(0), "ks"), equalTo(List.<Object>of("one")));
        assertThat(rows.get(0).getString("k", 0), equalTo("fine"));
    }
}
