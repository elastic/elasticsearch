/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.ParquetCodecs;
import org.elasticsearch.xpack.parquet.write.file.ParquetReadOptionsFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Spike S1: the engine shell with local Parquet files. Documents go through the batch indexing path, each shard bulk writes its files
 * before it returns, nothing can be searched, and a restart finds the files again.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ParquetWriteEngineIT extends ESIntegTestCase {

    private static final String INDEX = "parquet-index";
    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ParquetWriteEnginePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        // The mock engine factory is registered for every index, and an index can have only one engine factory.
        return false;
    }

    @Override
    protected void beforeIndexDeletion() {
        // The base class compares the documents of the copies of every shard with searches, and this engine cannot be searched.
    }

    private Settings.Builder parquetSettings() {
        return Settings.builder()
            .put("index.storage_format", "parquet")
            .put("index.mode", "columnar")
            .put("index.number_of_shards", 1)
            .put("index.parquet.repository", "not-used-before-the-upload-step")
            .put("index.parquet.local_compression", randomFrom("uncompressed", "snappy", "zstd"));
    }

    private void createParquetIndex() throws IOException {
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
        assertAcked(indicesAdmin().prepareCreate(INDEX).setSettings(parquetSettings()).setMapping(mapping));
        ensureGreen(INDEX);
    }

    private BulkResponse bulk(int from, int to, WriteRequest.RefreshPolicy refreshPolicy) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(refreshPolicy);
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

    private IndexShard shard() {
        final IndicesService indicesService = internalCluster().getDataNodeInstance(IndicesService.class);
        final Index index = resolveIndex(INDEX);
        return indicesService.indexServiceSafe(index).getShard(0);
    }

    /** Reads every row of every local file of the shard, and checks that the sequence number ranges tile the rows. */
    private List<Group> readLocalRows() throws IOException {
        final Directory directory = shard().store().directory();
        final List<Group> rows = new java.util.ArrayList<>();
        long expectedFrom = 0;
        for (LocalParquetFiles.LocalFile file : LocalParquetFiles.list(directory)) {
            assertThat("files must be contiguous", file.fromSeqNo(), equalTo(expectedFrom));
            expectedFrom = file.toSeqNo() + 1;
            final byte[] bytes = new byte[(int) file.length()];
            try (IndexInput in = directory.openInput(file.name(), IOContext.READONCE)) {
                in.readBytes(bytes, 0, bytes.length);
            }
            int read = 0;
            final ParquetReadOptions options = ParquetReadOptionsFactory.create(new ParquetCodecs());
            try (ParquetFileReader reader = ParquetFileReader.open(new BytesInputFile(bytes), options)) {
                final MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (long i = 0; i < pages.getRowCount(); i++) {
                        rows.add(recordReader.read());
                        read++;
                    }
                }
            }
            assertThat("rows of " + file.name(), (long) read, equalTo(file.rows()));
        }
        return rows;
    }

    private void assertRows(int expectedRows) throws IOException {
        final List<Group> rows = readLocalRows();
        assertThat(rows.size(), equalTo(expectedRows));
        final Set<Integer> counts = new HashSet<>();
        for (Group row : rows) {
            final int count = (int) row.getLong("count", 0);
            assertTrue("count " + count + " is unique", counts.add(count));
            assertThat(row.getLong("@timestamp", 0), equalTo(BASE_TIMESTAMP + count * 1000L));
            assertThat(row.getString("host.name", 0), equalTo("host-" + (count % 7)));
            assertThat(row.getInteger("bytes", 0), equalTo(count % 1000));
        }
        assertThat(counts.size(), equalTo(expectedRows));
    }

    public void testBatchesAreWrittenAsLocalParquetFiles() throws Exception {
        createParquetIndex();

        // More than 5,000 documents in one shard bulk make several batches.
        BulkResponse response = bulk(0, 12_000, WriteRequest.RefreshPolicy.NONE);
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        response = bulk(12_000, 20_000, WriteRequest.RefreshPolicy.NONE);
        assertFalse(response.buildFailureMessage(), response.hasFailures());

        assertRows(20_000);
        assertThat(LocalParquetFiles.list(shard().store().directory()).size(), greaterThan(1));
        assertThat(shard().seqNoStats().getMaxSeqNo(), equalTo(19_999L));
        assertThat(shard().seqNoStats().getLocalCheckpoint(), equalTo(19_999L));
        assertThat(shard().docStats().getCount(), equalTo(20_000L));
    }

    @org.elasticsearch.test.junit.annotations.TestLogging(
        reason = "debug the wait_for hang",
        value = "org.elasticsearch.index.shard:TRACE,org.elasticsearch.index:DEBUG"
    )
    public void testRefreshAndStatsWork() throws Exception {
        createParquetIndex();
        // refresh=wait_for uploads the rows (see ParquetSearchReplicaIT), and this index has no repository
        final BulkResponse response = bulk(0, 100, WriteRequest.RefreshPolicy.NONE);
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        assertNoFailures(indicesAdmin().prepareRefresh(INDEX).get());
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(100L));
    }

    public void testExplicitIdsDeletesAndUpdatesAreRejectedPerItem() throws Exception {
        createParquetIndex();
        final BulkResponse mixed = client().prepareBulk()
            .add(new IndexRequest(INDEX).source("@timestamp", BASE_TIMESTAMP, "host.name", "a", "count", 1, "bytes", 1))
            .add(new IndexRequest(INDEX).id("explicit").source("@timestamp", BASE_TIMESTAMP, "host.name", "b", "count", 2, "bytes", 2))
            .get();
        assertFalse(mixed.getItems()[0].isFailed());
        assertTrue(mixed.getItems()[1].isFailed());
        assertThat(mixed.getItems()[1].getFailureMessage(), containsString("auto-generated ids"));

        final BulkResponse delete = client().prepareBulk().add(client().prepareDelete(INDEX, "x").request()).get();
        assertTrue(delete.getItems()[0].isFailed());
        assertThat(delete.getItems()[0].getFailureMessage(), containsString("does not delete"));

        final BulkResponse update = client().prepareBulk().add(client().prepareUpdate(INDEX, "x").setDoc("count", 3).request()).get();
        assertTrue(update.getItems()[0].isFailed());

        // Only the one accepted document is in a file.
        final List<Group> rows = readLocalRows();
        assertThat(rows.size(), equalTo(1));
        assertThat(rows.get(0).getLong("count", 0), equalTo(1L));
    }

    /**
     * A document with a field that the mapping does not have takes the whole shard bulk off the batch path: the batch mapper handles
     * only mapped columns, so the bulk is parsed one document at a time, and then the engine rejects every document, because it accepts
     * only batches. This is a known limit of the prototype, and the valid documents of a bulk without such a document are unaffected.
     */
    public void testUnmappedFieldTakesTheBulkOffTheBatchPath() throws Exception {
        createParquetIndex();
        final BulkResponse response = client().prepareBulk()
            .add(new IndexRequest(INDEX).source("@timestamp", BASE_TIMESTAMP, "host.name", "a", "count", 1, "bytes", 1))
            .add(new IndexRequest(INDEX).source("@timestamp", BASE_TIMESTAMP, "unknown", "x"))
            .get();
        assertTrue(response.buildFailureMessage(), response.getItems()[0].isFailed());
        assertThat(response.getItems()[0].getFailureMessage(), containsString("only batches"));
        assertTrue(response.getItems()[1].isFailed());
        assertThat(response.getItems()[1].getFailureMessage(), containsString("strict"));
        assertThat(readLocalRows().size(), equalTo(0));

        // The same documents without the unmapped field are written.
        assertFalse(bulk(0, 10, WriteRequest.RefreshPolicy.NONE).hasFailures());
        assertThat(readLocalRows().size(), equalTo(10));
    }

    public void testASearchNeverReachesThePrimary() throws Exception {
        createParquetIndex();
        bulk(0, 10, WriteRequest.RefreshPolicy.NONE);
        // the primary is not searchable and the index has no search replica, so no copy answers
        final Exception e = expectThrows(Exception.class, () -> prepareSearch(INDEX).get());
        assertThat(ExceptionsHelper.stackTrace(e), containsString("NoShardAvailableActionException"));
    }

    public void testFilesSurviveAFullRestart() throws Exception {
        createParquetIndex();
        assertFalse(bulk(0, 7_000, WriteRequest.RefreshPolicy.NONE).hasFailures());
        internalCluster().fullRestart();
        ensureGreen(INDEX);
        assertRows(7_000);
        assertThat(shard().seqNoStats().getMaxSeqNo(), equalTo(6_999L));
        // New documents continue the sequence.
        assertFalse(bulk(7_000, 7_500, WriteRequest.RefreshPolicy.NONE).hasFailures());
        assertRows(7_500);
        assertThat(shard().seqNoStats().getMaxSeqNo(), equalTo(7_499L));
    }

    public void testIndexCreationRules() throws Exception {
        // not columnar
        expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("not-columnar")
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.parquet.repository", "r")
                        .put("index.number_of_shards", 1)
                )
                .get()
        );
        // auto-expanded replicas (a fixed number of replicas is fine: they are search replicas)
        final Exception replicas = expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("replicas").setSettings(parquetSettings().put("index.auto_expand_replicas", "0-1")).get()
        );
        assertThat(ExceptionsHelper.stackTrace(replicas), containsString("does not support [index.auto_expand_replicas]"));
        // no repository
        expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("no-repository")
                .setSettings(
                    Settings.builder().put("index.storage_format", "parquet").put("index.mode", "columnar").put("index.number_of_shards", 1)
                )
                .get()
        );
        // the settings that the format needs are accepted when the request sets them to the needed value ...
        assertAcked(
            indicesAdmin().prepareCreate("explicit")
                .setSettings(
                    parquetSettings().put("index.number_of_replicas", 0)
                        .put("index.batch_indexing", true)
                        .put("index.mapping.doc_values.multi_value", false)
                        .put("index.mapping.source.mode", "synthetic")
                )
                .setMapping("count", "type=long")
        );
        // ... and refused, with a message that names the format, when it sets them to a value that the format cannot take
        final Exception batching = expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("no-batching").setSettings(parquetSettings().put("index.batch_indexing", false)).get()
        );
        assertThat(ExceptionsHelper.stackTrace(batching), containsString("requires [index.batch_indexing] [true]"));
        // a field with several values is a list, so the index default may be true, but a boolean cannot have several values
        assertAcked(
            indicesAdmin().prepareCreate("lists")
                .setSettings(parquetSettings().put("index.mapping.doc_values.multi_value", true))
                .setMapping("tags", "type=keyword", "flag", "type=boolean,multi_value=false", "when", "type=date,multi_value=false")
        );
        final Exception multiBoolean = expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("multi-boolean")
                .setSettings(parquetSettings().put("index.mapping.doc_values.multi_value", true))
                .setMapping("flag", "type=boolean")
                .get()
        );
        assertThat(ExceptionsHelper.stackTrace(multiBoolean), containsString("does not support a boolean field with several values"));
        final Exception multiDate = expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("multi-date")
                .setSettings(parquetSettings())
                .setMapping("when", "type=date,multi_value=true")
                .get()
        );
        assertThat(ExceptionsHelper.stackTrace(multiDate), containsString("does not support a date field with several values"));
        final Exception sourceMode = expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("stored-source")
                .setSettings(parquetSettings().put("index.mapping.source.mode", "columnar_stored"))
                .get()
        );
        assertThat(ExceptionsHelper.stackTrace(sourceMode), containsString("requires [index.mapping.source.mode] [synthetic]"));
        // a mapping that the engine cannot store is refused when the index is created, with the field and the type in the message
        final Exception nanos = expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("nanos").setSettings(parquetSettings()).setMapping("when", "type=date_nanos").get()
        );
        assertThat(ExceptionsHelper.stackTrace(nanos), containsString("does not support the type [date_nanos] of the field [when]"));
        final Exception indexed = expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("indexed").setSettings(parquetSettings()).setMapping("k", "type=keyword,index=true").get()
        );
        assertThat(ExceptionsHelper.stackTrace(indexed), containsString("does not support an indexed field"));
        // the namespace of the catalog is a path of the repository, so a slash or a space is refused
        final Exception namespace = expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("bad-namespace")
                .setSettings(parquetSettings().put("index.parquet.catalog.namespace", "a/b"))
                .get()
        );
        assertThat(ExceptionsHelper.stackTrace(namespace), containsString("index.parquet.catalog.namespace"));
        // a native index keeps working and has the default engine
        assertAcked(indicesAdmin().prepareCreate("native").setSettings(Settings.builder().put("index.number_of_shards", 1)));
    }

    /** A Parquet file in memory. */
    private record BytesInputFile(byte[] bytes) implements InputFile {
        @Override
        public long getLength() {
            return bytes.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new SeekableInputStream() {
                private final ByteBuffer buffer = ByteBuffer.wrap(bytes);

                @Override
                public long getPos() {
                    return buffer.position();
                }

                @Override
                public void seek(long newPos) {
                    buffer.position((int) newPos);
                }

                @Override
                public void readFully(byte[] bytes) throws IOException {
                    readFully(bytes, 0, bytes.length);
                }

                @Override
                public void readFully(byte[] bytes, int start, int len) throws IOException {
                    if (buffer.remaining() < len) {
                        throw new EOFException();
                    }
                    buffer.get(bytes, start, len);
                }

                @Override
                public int read(ByteBuffer destination) {
                    if (buffer.hasRemaining() == false) {
                        return -1;
                    }
                    final int length = Math.min(destination.remaining(), buffer.remaining());
                    final ByteBuffer slice = buffer.slice().limit(length);
                    destination.put(slice);
                    buffer.position(buffer.position() + length);
                    return length;
                }

                @Override
                public void readFully(ByteBuffer destination) throws IOException {
                    if (buffer.remaining() < destination.remaining()) {
                        throw new EOFException();
                    }
                    read(destination);
                }

                @Override
                public int read() {
                    return buffer.hasRemaining() ? buffer.get() & 0xFF : -1;
                }
            };
        }
    }
}
