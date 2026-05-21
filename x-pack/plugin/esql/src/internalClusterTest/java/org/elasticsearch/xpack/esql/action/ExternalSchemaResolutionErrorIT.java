/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests verifying that STRICT schema resolution rejects multi-file queries
 * when file schemas are incompatible. These cases produce HTTP 400 errors that cannot be
 * asserted in the csv-spec runner (which has no error-assertion syntax).
 */
public class ExternalSchemaResolutionErrorIT extends AbstractEsqlIntegTestCase {

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
        plugins.add(CsvDataSourcePlugin.class);
        plugins.add(ParquetDataSourcePlugin.class);
        return plugins;
    }

    public void testStrictOnDivergentCsvTypedHeaders() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path dir = createTempDir().resolve("strict_csv_typed");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "name:keyword,age:integer\nalice,30\nbob,40\n", StandardCharsets.UTF_8);
        Files.writeString(
            dir.resolve("b.csv"),
            "age:long,name:keyword,city:keyword\n50,charlie,paris\n60,dave,london\n",
            StandardCharsets.UTF_8
        );

        String glob = StoragePath.fileUri(dir) + "/*.csv";
        String query = "EXTERNAL \""
            + glob
            + "\" WITH {\"schema_resolution\": \"strict\", \"header_row\": true}"
            + " | STATS count = COUNT(*)";

        var ex = expectThrows(Exception.class, () -> {
            try (var response = run(syncEsqlQueryRequest(query))) {
                // should not reach here
            }
        });
        assertThat(ex.getMessage(), containsString("strict"));
    }

    public void testStrictOnDivergentCsvHeaderless() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path dir = createTempDir().resolve("strict_csv_headerless");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "abc,1\nxyz,2\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "10,10\n20,20\n30,30\n", StandardCharsets.UTF_8);

        String glob = StoragePath.fileUri(dir) + "/*.csv";
        String query = "EXTERNAL \""
            + glob
            + "\" WITH {\"schema_resolution\": \"strict\", \"header_row\": false}"
            + " | STATS count = COUNT(*)";

        var ex = expectThrows(Exception.class, () -> {
            try (var response = run(syncEsqlQueryRequest(query))) {
                // should not reach here
            }
        });
        assertThat(ex.getMessage(), containsString("strict"));
    }

    public void testStrictOnCsvTypeDrift() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path dir = createTempDir().resolve("strict_csv_type_drift");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,col,note\n1,123,alpha\n2,456,gamma\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,col,note\n4,abc,beta\n5,def,epsilon\n", StandardCharsets.UTF_8);

        String glob = StoragePath.fileUri(dir) + "/*.csv";
        String query = "EXTERNAL \""
            + glob
            + "\" WITH {\"schema_resolution\": \"strict\", \"header_row\": true}"
            + " | STATS count = COUNT(*)";

        var ex = expectThrows(Exception.class, () -> {
            try (var response = run(syncEsqlQueryRequest(query))) {
                // should not reach here
            }
        });
        assertThat(ex.getMessage(), containsString("strict"));
    }

    public void testStrictOnDivergentParquet() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path dir = createTempDir().resolve("strict_parquet_divergent");
        Files.createDirectories(dir);

        MessageType schemaA = MessageTypeParser.parseMessageType(
            "message schema {\n" + "  optional binary name (UTF8);\n" + "  optional int32 age;\n" + "  optional double score;\n" + "}"
        );
        writeParquetFile(dir.resolve("a.parquet"), schemaA, List.of(new Object[] { "alice", 30, 85.5 }, new Object[] { "bob", 40, 92.0 }));

        MessageType schemaB = MessageTypeParser.parseMessageType(
            "message schema {\n" + "  optional int64 age;\n" + "  optional binary name (UTF8);\n" + "  optional binary city (UTF8);\n" + "}"
        );
        writeParquetFile(
            dir.resolve("b.parquet"),
            schemaB,
            List.of(new Object[] { 50L, "charlie", "paris" }, new Object[] { 60L, "dave", "london" })
        );

        String glob = StoragePath.fileUri(dir) + "/*.parquet";
        String query = "EXTERNAL \"" + glob + "\" WITH {\"schema_resolution\": \"strict\"}" + " | STATS count = COUNT(*)";

        var ex = expectThrows(Exception.class, () -> {
            try (var response = run(syncEsqlQueryRequest(query))) {
                // should not reach here
            }
        });
        assertThat(ex.getMessage(), containsString("strict"));
    }

    private static void writeParquetFile(Path path, MessageType schema, List<Object[]> rows) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (Object[] row : rows) {
                Group g = factory.newGroup();
                for (int i = 0; i < schema.getFieldCount(); i++) {
                    Object value = row[i];
                    if (value == null) {
                        continue;
                    }
                    switch (schema.getType(i).asPrimitiveType().getPrimitiveTypeName()) {
                        case BINARY -> g.add(schema.getFieldName(i), value.toString());
                        case INT32 -> g.add(schema.getFieldName(i), ((Number) value).intValue());
                        case INT64 -> g.add(schema.getFieldName(i), ((Number) value).longValue());
                        case DOUBLE -> g.add(schema.getFieldName(i), ((Number) value).doubleValue());
                        case FLOAT -> g.add(schema.getFieldName(i), ((Number) value).floatValue());
                        case BOOLEAN -> g.add(schema.getFieldName(i), (Boolean) value);
                        default -> throw new IllegalArgumentException("Unsupported type: " + schema.getType(i));
                    }
                }
                writer.write(g);
            }
        }
        Files.write(path, baos.toByteArray());
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

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
}
