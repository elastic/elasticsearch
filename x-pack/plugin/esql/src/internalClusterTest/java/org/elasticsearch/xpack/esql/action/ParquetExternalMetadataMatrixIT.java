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
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * Parquet binding of the standard-metadata matrix. Parquet is the only {@code ColumnExtractorAware}
 * reader, which means its {@code _rowPosition} channel is the deferred-extraction-encoded form
 * ({@code (extractorId << LOCAL_POSITION_BITS) | physicalPosition}); {@code ExternalRowIdentity}
 * masks the high bits off before composing {@code _id}. This subclass exercises that masked-compose
 * path; the shared base assertion is the opaque-token contract (distinct, non-negative, increasing
 * in file order, common location prefix) since each reader's token form is format-defined.
 *
 * <p>The existing {@code iceberg-fixtures/standalone/employees.parquet} is 100 rows with a complex
 * schema and no keyword {@code host_ip} column, so it cannot back the 3-row matrix fixture; this
 * writes a fresh 3-row file with the canonical {@code emp_no/first_name/host_ip} shape instead.
 */
public class ParquetExternalMetadataMatrixIT extends AbstractExternalMetadataMatrixIT {

    @Override
    protected String format() {
        return "parquet";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected String writeFixture(Path dir) throws Exception {
        // int32 -> INTEGER, binary (UTF8) -> KEYWORD. Single row group keeps the three rows in one
        // file at offsets 0,1,2.
        MessageType schema = MessageTypeParser.parseMessageType(
            "message employees { required int32 emp_no; required binary first_name (UTF8); required binary host_ip (UTF8); }"
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        String[] firstNames = { "Alice", "Bob", "Carol" };
        String[] hostIps = { "10.0.0.1", "10.0.0.2", "10.0.0.3" };

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < 3; i++) {
                Group g = factory.newGroup();
                g.add("emp_no", i + 1);
                g.add("first_name", firstNames[i]);
                g.add("host_ip", hostIps[i]);
                writer.write(g);
            }
        }

        Path file = dir.resolve("employees.parquet");
        Files.write(file, baos.toByteArray());
        return StoragePath.fileUri(file);
    }

    /** In-memory {@link OutputFile} so the writer streams into a byte buffer we then flush to disk. */
    private static OutputFile createOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionStream(baos);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return positionStream(baos);
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

    private static PositionOutputStream positionStream(ByteArrayOutputStream baos) {
        return new PositionOutputStream() {
            private long position = 0;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int b) {
                baos.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                baos.write(b, off, len);
                position += len;
            }
        };
    }
}
