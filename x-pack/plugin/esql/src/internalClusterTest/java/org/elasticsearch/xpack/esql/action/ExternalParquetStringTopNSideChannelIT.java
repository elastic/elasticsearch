/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.function.IntFunction;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end coverage for the {@code BYTES_REF} TopN side-channel: a single keyword sort key over an
 * external Parquet source publishes its competitive string bound to the format reader, which prunes
 * row groups that cannot contain a globally competitive row. This is the string counterpart to
 * {@link ExternalParquetNumericTopNSideChannelIT}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ExternalParquetStringTopNSideChannelIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue LONG_TIMEOUT = TimeValue.timeValueMinutes(2);

    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(ParquetDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testRowGroupSkipAscendingMonotonicData() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path file = writeParquetFile(1_000, 1L, 2 * 1024 * 1024, ExternalParquetStringTopNSideChannelIT::key);
        try {
            // Correctness only: whether any row group is physically skipped depends on the async
            // source's prefetch racing the competitive bound (the 10-page buffer can hold the whole
            // small file before TopN publishes a bound), so reduced-read metrics are non-deterministic
            // here. Deterministic row-group pruning is proven by OptimizedParquetDynamicThresholdTests.
            QueryResult result = runTopN(file, "name ASC", 10);
            assertNames(result, 0, 10);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testRowGroupSkipDescendingMonotonicData() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path file = writeParquetFile(1_000, 1L, 2 * 1024 * 1024, ExternalParquetStringTopNSideChannelIT::key);
        try {
            QueryResult result = runTopN(file, "name DESC", 10);
            assertDescendingNames(result, 999, 10);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testSinglePageRowGroupNegativeControlReadsWholeGroup() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path file = writeParquetFile(2_000, 64L * 1024 * 1024, 2 * 1024 * 1024, ExternalParquetStringTopNSideChannelIT::key);
        try {
            QueryResult result = runTopN(file, "name ASC", 10);
            assertNames(result, 0, 10);
            assertThat(result.documentsFound(), greaterThan(1_500L));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testNullsFirstEarlyTermination() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path file = writeParquetFile(1_000, 1L, 2 * 1024 * 1024, i -> i < 100 ? null : key(i));
        try {
            QueryResult result = runTopN(file, "name ASC NULLS FIRST", 50);
            assertEquals(50, result.names().size());
            for (String name : result.names()) {
                assertNull(name);
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testNullsLastDoesNotTriggerEarlyTermination() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path file = writeParquetFile(1_000, 1L, 2 * 1024 * 1024, i -> i < 100 ? null : key(i));
        try {
            QueryResult result = runTopN(file, "name ASC NULLS LAST", 10);
            assertNames(result, 100, 10);
            assertThat(result.documentsFound(), greaterThan(10L));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testOverlappingRowGroupsKeepCorrectness() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path file = writeParquetFile(500, 1L, 2 * 1024 * 1024, i -> key((i * 37) % 1_000));
        try {
            QueryResult result = runTopN(file, "name ASC", 50);
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                expected.add(key((i * 37) % 1_000));
            }
            expected.sort(String::compareTo);
            assertEquals(expected.subList(0, 50), result.names());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private QueryResult runTopN(Path file, String order, int limit) throws IOException {
        String query = "EXTERNAL \"" + StoragePath.fileUri(file) + "\" | SORT " + order + " | LIMIT " + limit + " | KEEP name, payload";
        var request = syncEsqlQueryRequest(query);
        request.profile(true);
        try (var response = run(request, LONG_TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            List<String> names = new ArrayList<>(rows.size());
            for (List<Object> row : rows) {
                String name = row.get(0) == null ? null : bytesRefToString(row.get(0));
                names.add(name);
                if (name != null) {
                    assertEquals(payload(name), bytesRefToString(row.get(1)));
                }
            }
            return new QueryResult(names, response.documentsFound());
        }
    }

    private static void assertNames(QueryResult result, int startInclusive, int count) {
        assertEquals(count, result.names().size());
        for (int i = 0; i < count; i++) {
            assertEquals(key(startInclusive + i), result.names().get(i));
        }
    }

    private static void assertDescendingNames(QueryResult result, int startInclusive, int count) {
        assertEquals(count, result.names().size());
        for (int i = 0; i < count; i++) {
            assertEquals(key(startInclusive - i), result.names().get(i));
        }
    }

    private Path writeParquetFile(int rowCount, long rowGroupSize, int pageSize, IntFunction<String> valueForPosition) throws IOException {
        Path tempFile = createTempDir().resolve("string_topn_side_channel.parquet");
        MessageType schema = MessageTypeParser.parseMessageType(
            "message test { optional binary name (UTF8); required binary payload (UTF8); }"
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(pageSize)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                String name = valueForPosition.apply(i);
                Group group = factory.newGroup();
                if (name != null) {
                    group.add("name", name);
                    group.add("payload", payload(name));
                } else {
                    group.add("payload", "payload_null_" + i);
                }
                writer.write(group);
            }
        }
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        baos.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        baos.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    private static String bytesRefToString(Object cell) {
        if (cell instanceof BytesRef br) {
            return br.utf8ToString();
        }
        return String.valueOf(cell);
    }

    private static String key(int position) {
        return String.format(Locale.ROOT, "key%06d", position);
    }

    private static String payload(String name) {
        return "payload_" + name;
    }

    private record QueryResult(List<String> names, long documentsFound) {}
}
