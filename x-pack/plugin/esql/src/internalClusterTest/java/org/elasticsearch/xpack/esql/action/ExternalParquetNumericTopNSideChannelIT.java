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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
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
import java.util.Map;
import java.util.function.IntFunction;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ExternalParquetNumericTopNSideChannelIT extends AbstractExternalDataSourceIT {

    private static final TimeValue LONG_TIMEOUT = TimeValue.timeValueMinutes(2);

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testRowGroupSkipAscendingMonotonicData() throws Exception {
        Path file = writeParquetFile(1_000, 1L, 2 * 1024 * 1024, i -> (long) i);
        try {
            QueryResult result = runTopN(file, "id ASC", 10);
            assertIds(result, 0, 10);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testRowGroupSkipDescendingMonotonicData() throws Exception {
        Path file = writeParquetFile(1_000, 1L, 2 * 1024 * 1024, i -> 999L - i);
        try {
            QueryResult result = runTopN(file, "id DESC", 10);
            assertDescendingIds(result, 999, 10);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testPageLevelSkipWithinSingleRowGroup() throws Exception {
        Path file = writeParquetFile(2_000, 64L * 1024 * 1024, 64, i -> (long) i);
        try {
            QueryResult result = runTopN(file, "id ASC", 10);
            assertIds(result, 0, 10);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testSinglePageRowGroupNegativeControlReadsWholeGroup() throws Exception {
        Path file = writeParquetFile(2_000, 64L * 1024 * 1024, 2 * 1024 * 1024, i -> (long) i);
        try {
            QueryResult result = runTopN(file, "id ASC", 10);
            assertIds(result, 0, 10);
            assertThat(result.documentsFound(), greaterThan(1_500L));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testNullsFirstEarlyTermination() throws Exception {
        Path file = writeParquetFile(1_000, 1L, 2 * 1024 * 1024, i -> i < 100 ? null : (long) i);
        try {
            QueryResult result = runTopN(file, "id ASC NULLS FIRST", 50);
            assertEquals(50, result.ids().size());
            for (Long id : result.ids()) {
                assertNull(id);
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testNullsLastDoesNotTriggerEarlyTermination() throws Exception {
        Path file = writeParquetFile(1_000, 1L, 2 * 1024 * 1024, i -> i < 100 ? null : (long) i);
        try {
            QueryResult result = runTopN(file, "id ASC NULLS LAST", 10);
            assertIds(result, 100, 10);
            assertThat(result.documentsFound(), greaterThan(10L));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testOverlappingRowGroupsKeepCorrectness() throws Exception {
        Path file = writeParquetFile(500, 1L, 2 * 1024 * 1024, i -> (long) ((i * 37) % 1_000));
        try {
            QueryResult result = runTopN(file, "id ASC", 50);
            List<Long> expected = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                expected.add((long) ((i * 37) % 1_000));
            }
            expected.sort(Long::compareTo);
            assertEquals(expected.subList(0, 50), result.ids());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private QueryResult runTopN(Path file, String order, int limit) throws IOException {
        String dataset = registerDataset("numeric_topn_sc", StoragePath.fileUri(file), Map.of());
        String query = "FROM " + dataset + " | SORT " + order + " | LIMIT " + limit + " | KEEP id, payload";
        var request = syncEsqlQueryRequest(query);
        request.profile(true);
        try (var response = run(request, LONG_TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            List<Long> ids = new ArrayList<>(rows.size());
            for (List<Object> row : rows) {
                ids.add(row.get(0) == null ? null : ((Number) row.get(0)).longValue());
                Long id = ids.get(ids.size() - 1);
                if (id != null) {
                    assertEquals(payload(id), bytesRefToString(row.get(1)));
                }
            }
            return new QueryResult(ids, response.documentsFound());
        }
    }

    private static void assertIds(QueryResult result, long startInclusive, int count) {
        assertEquals(count, result.ids().size());
        for (int i = 0; i < count; i++) {
            assertEquals(startInclusive + i, result.ids().get(i).longValue());
        }
    }

    private static void assertDescendingIds(QueryResult result, long startInclusive, int count) {
        assertEquals(count, result.ids().size());
        for (int i = 0; i < count; i++) {
            assertEquals(startInclusive - i, result.ids().get(i).longValue());
        }
    }

    private Path writeParquetFile(int rowCount, long rowGroupSize, int pageSize, IntFunction<Long> valueForPosition) throws IOException {
        Path tempFile = createTempDir().resolve("numeric_topn_side_channel.parquet");
        MessageType schema = MessageTypeParser.parseMessageType("message test { optional int64 id; required binary payload (UTF8); }");
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
                Long id = valueForPosition.apply(i);
                Group group = factory.newGroup();
                if (id != null) {
                    group.add("id", id);
                    group.add("payload", payload(id));
                } else {
                    group.add("payload", "payload_null_" + i);
                }
                writer.write(group);
            }
        }
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    private static String bytesRefToString(Object cell) {
        if (cell instanceof BytesRef br) {
            return br.utf8ToString();
        }
        return String.valueOf(cell);
    }

    private static String payload(long id) {
        return "payload_" + id;
    }

    private record QueryResult(List<Long> ids, long documentsFound) {}
}
