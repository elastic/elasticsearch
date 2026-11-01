/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end regression for the Parquet MAP footer-stats crash. A Parquet {@code MAP<K,V>} has two
 * physical leaves ({@code m.key_value.key}, {@code m.key_value.value}) that the flattener collapses to
 * the same logical name; folding their heterogeneously typed footer stats into one entry threw a
 * {@link ClassCastException} in metadata extraction, surfacing as an HTTP 500 before type resolution —
 * so even {@code STATS COUNT(*)} over a file containing a MAP column failed. This test writes a file
 * with a MAP column alongside a scalar and a LIST control and asserts the queries succeed.
 */
public class ExternalParquetMapColumnIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testCountStarOverMapColumnDoesNotCrash() throws Exception {
        int totalRows = 200;
        Path parquetFile = writeMapParquetFile(totalRows);
        try {
            String dataset = registerDataset("map_column", StoragePath.fileUri(parquetFile), Map.of());

            // COUNT(*): before the fix this failed with a 500 during metadata extraction of the MAP leaves.
            try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"))) {
                List<? extends ColumnInfo> columns = response.columns();
                assertThat(columns.size(), equalTo(1));
                assertThat(columns.get(0).name(), equalTo("c"));
                List<List<Object>> rows = getValuesList(response);
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo((long) totalRows));
            }

            // The sibling scalar column still reads.
            try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id | STATS mx = MAX(id)"))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo((long) (totalRows - 1)));
            }

            // The LIST control column still reads.
            try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(tags)"))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo((long) totalRows));
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    private Path writeMapParquetFile(int rowCount) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("""
            message test {
              required int64 id;
              optional group m (MAP) {
                repeated group key_value {
                  required binary key (UTF8);
                  optional int64 value;
                }
              }
              optional group tags (LIST) {
                repeated group list {
                  optional binary element (UTF8);
                }
              }
            }
            """);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                Group m = g.addGroup("m");
                m.addGroup("key_value").append("key", "k" + i).append("value", (long) (i * 10));
                m.addGroup("key_value").append("key", "j" + i).append("value", (long) (i * 10 + 1));
                Group tags = g.addGroup("tags");
                tags.addGroup("list").append("element", "t" + i);
                writer.write(g);
            }
        }

        Path tempFile = createTempDir().resolve("map_column_test.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }
}
