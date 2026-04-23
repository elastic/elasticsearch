/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.nativeaccess.ParquetRsFunctions;
import org.elasticsearch.parquetrs.ParquetRs;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class ParquetRsNativeTests extends ESTestCase {

    static ParquetRsFunctions parquetRs;

    @BeforeClass
    public static void loadRustLib() {
        var opt = ParquetRs.functions();
        assumeTrue("parquet-rs native library not available", opt.isPresent());
        parquetRs = opt.get();
    }

    // -- Happy-path tests (null configJson = local file) --

    public void testGetStatistics() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("test_schema");

        Path parquetFile = writeTempParquet(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("id", 1L);
            g1.add("name", "Alice");
            Group g2 = factory.newGroup();
            g2.add("id", 2L);
            g2.add("name", "Bob");
            Group g3 = factory.newGroup();
            g3.add("id", 3L);
            g3.add("name", "Charlie");
            return List.of(g1, g2, g3);
        });

        long[] stats = parquetRs.getStatistics(parquetFile.toAbsolutePath().toString(), null);
        assertThat(stats, notNullValue());
        assertThat(stats[0], equalTo(3L));
        assertThat(stats[1], greaterThan(0L));
    }

    public void testGetSchemaFFI() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("active")
            .named("test_schema");

        Path parquetFile = writeTempParquet(schema, factory -> {
            Group g = factory.newGroup();
            g.add("id", 1L);
            g.add("name", "Alice");
            g.add("score", 95.5);
            g.add("active", true);
            return List.of(g);
        });

        try (BufferAllocator allocator = new RootAllocator(); ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            parquetRs.getSchemaFFI(parquetFile.toAbsolutePath().toString(), null, ffiSchema.memoryAddress());
            Schema arrowSchema = Data.importSchema(allocator, ffiSchema, null);
            List<Field> fields = arrowSchema.getFields();
            assertThat(fields.size(), equalTo(4));
            assertThat(fields.get(0).getName(), equalTo("id"));
            assertThat(fields.get(1).getName(), equalTo("name"));
            assertThat(fields.get(2).getName(), equalTo("score"));
            assertThat(fields.get(3).getName(), equalTo("active"));
        }
    }

    public void testGetSchemaFFIAndStatisticsTogether() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("user_id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("email")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .named("users_schema");

        Path parquetFile = writeTempParquet(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("user_id", 100L);
            g1.add("email", "alice@example.com");
            g1.add("age", 30);
            Group g2 = factory.newGroup();
            g2.add("user_id", 200L);
            g2.add("email", "bob@example.com");
            g2.add("age", 25);
            return List.of(g1, g2);
        });

        String absPath = parquetFile.toAbsolutePath().toString();
        long[] stats = parquetRs.getStatistics(absPath, null);
        assertThat(stats, notNullValue());
        assertThat(stats[0], equalTo(2L));
        assertThat(stats[1], greaterThan(0L));

        try (BufferAllocator allocator = new RootAllocator(); ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            parquetRs.getSchemaFFI(absPath, null, ffiSchema.memoryAddress());
            Schema arrowSchema = Data.importSchema(allocator, ffiSchema, null);
            assertThat(arrowSchema.getFields().size(), equalTo(3));
            assertThat(arrowSchema.getFields().get(0).getName(), equalTo("user_id"));
            assertThat(arrowSchema.getFields().get(1).getName(), equalTo("email"));
            assertThat(arrowSchema.getFields().get(2).getName(), equalTo("age"));
        }
    }

    // -- configJson: null vs empty JSON vs valid JSON for local files --

    public void testGetStatisticsWithEmptyConfigJson() throws Exception {
        Path parquetFile = writeSimpleParquetFile();
        long[] stats = parquetRs.getStatistics(parquetFile.toAbsolutePath().toString(), "{}");
        assertThat(stats, notNullValue());
        assertThat(stats[0], equalTo(1L));
    }

    public void testGetStatisticsWithValidConfigJson() throws Exception {
        Path parquetFile = writeSimpleParquetFile();
        long[] stats = parquetRs.getStatistics(parquetFile.toAbsolutePath().toString(), "{\"key\": \"value\"}");
        assertThat(stats, notNullValue());
        assertThat(stats[0], equalTo(1L));
    }

    public void testGetSchemaFFIWithEmptyConfigJson() throws Exception {
        Path parquetFile = writeSimpleParquetFile();
        try (BufferAllocator allocator = new RootAllocator(); ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            parquetRs.getSchemaFFI(parquetFile.toAbsolutePath().toString(), "{}", ffiSchema.memoryAddress());
            Schema arrowSchema = Data.importSchema(allocator, ffiSchema, null);
            assertThat(arrowSchema.getFields().size(), equalTo(1));
        }
    }

    public void testGetSchemaFFIWithValidConfigJson() throws Exception {
        Path parquetFile = writeSimpleParquetFile();
        try (BufferAllocator allocator = new RootAllocator(); ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            parquetRs.getSchemaFFI(parquetFile.toAbsolutePath().toString(), "{\"key\": \"value\"}", ffiSchema.memoryAddress());
            Schema arrowSchema = Data.importSchema(allocator, ffiSchema, null);
            assertThat(arrowSchema.getFields().size(), equalTo(1));
        }
    }

    // -- configJson: malformed JSON --

    public void testGetStatisticsWithMalformedConfigJson() throws Exception {
        Path parquetFile = writeSimpleParquetFile();
        IOException e = expectThrows(
            IOException.class,
            () -> parquetRs.getStatistics(parquetFile.toAbsolutePath().toString(), "not valid json{{{")
        );
        assertThat(e.getMessage(), containsString("getStatistics failed"));
    }

    public void testGetSchemaFFIWithMalformedConfigJson() throws Exception {
        Path parquetFile = writeSimpleParquetFile();
        try (BufferAllocator allocator = new RootAllocator(); ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            IOException e = expectThrows(
                IOException.class,
                () -> parquetRs.getSchemaFFI(parquetFile.toAbsolutePath().toString(), "not valid json{{{", ffiSchema.memoryAddress())
            );
            assertThat(e.getMessage(), containsString("getSchemaFFI failed"));
        }
    }

    // -- Error handling tests --

    public void testGetStatisticsNonExistentFile() {
        IOException e = expectThrows(IOException.class, () -> parquetRs.getStatistics("/tmp/does_not_exist.parquet", null));
        assertThat(e.getMessage(), containsString("getStatistics failed"));
        assertThat(e.getMessage(), containsString("/tmp/does_not_exist.parquet"));
    }

    public void testGetSchemaFFINonExistentFile() {
        try (BufferAllocator allocator = new RootAllocator(); ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            IOException e = expectThrows(
                IOException.class,
                () -> parquetRs.getSchemaFFI("/tmp/does_not_exist.parquet", null, ffiSchema.memoryAddress())
            );
            assertThat(e.getMessage(), containsString("getSchemaFFI failed"));
            assertThat(e.getMessage(), containsString("/tmp/does_not_exist.parquet"));
        }
    }

    public void testGetStatisticsTruncatedParquet() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");
        Path parquetFile = writeTempParquet(schema, factory -> {
            Group g = factory.newGroup();
            g.add("id", 1L);
            return List.of(g);
        });
        byte[] fullBytes = Files.readAllBytes(parquetFile);
        Path truncated = createTempDir().resolve("truncated.parquet");
        Files.write(truncated, java.util.Arrays.copyOf(fullBytes, fullBytes.length / 2));
        IOException e = expectThrows(IOException.class, () -> parquetRs.getStatistics(truncated.toAbsolutePath().toString(), null));
        assertThat(e.getMessage(), containsString("getStatistics failed"));
    }

    public void testGetSchemaFFITruncatedParquet() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");
        Path parquetFile = writeTempParquet(schema, factory -> {
            Group g = factory.newGroup();
            g.add("id", 1L);
            return List.of(g);
        });

        byte[] fullBytes = Files.readAllBytes(parquetFile);
        Path truncated = createTempDir().resolve("truncated.parquet");
        Files.write(truncated, java.util.Arrays.copyOf(fullBytes, fullBytes.length / 2));
        try (BufferAllocator allocator = new RootAllocator(); ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            IOException e = expectThrows(
                IOException.class,
                () -> parquetRs.getSchemaFFI(truncated.toAbsolutePath().toString(), null, ffiSchema.memoryAddress())
            );
            assertThat(e.getMessage(), containsString("getSchemaFFI failed"));
        }
    }

    public void testGetStatisticsNotParquetFile() throws Exception {
        Path file = createTempDir().resolve("garbage.parquet");
        Files.writeString(file, "this is plain text, not a parquet file");
        IOException e = expectThrows(IOException.class, () -> parquetRs.getStatistics(file.toAbsolutePath().toString(), null));
        assertThat(e.getMessage(), containsString("getStatistics failed"));
    }

    public void testGetSchemaFFINotParquetFile() throws Exception {
        Path file = createTempDir().resolve("garbage.parquet");
        Files.writeString(file, "this is plain text, not a parquet file");
        try (BufferAllocator allocator = new RootAllocator(); ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            IOException e = expectThrows(
                IOException.class,
                () -> parquetRs.getSchemaFFI(file.toAbsolutePath().toString(), null, ffiSchema.memoryAddress())
            );
            assertThat(e.getMessage(), containsString("getSchemaFFI failed"));
        }
    }

    public void testErrorMessageIncludesNativeDetail() {
        IOException e = expectThrows(IOException.class, () -> parquetRs.getStatistics("/nonexistent/path/file.parquet", null));
        String msg = e.getMessage();
        assertThat(msg, containsString("getStatistics failed"));
        assertThat(msg, containsString("/nonexistent/path/file.parquet"));
        assertThat(msg, containsString("No such file or directory"));
    }

    // --- Test helpers ---

    /**
     * Creates the bytes for the test.parquet resource used by ParquetRsFunctionsTests.
     * Schema: {id: INT64, name: STRING}, 3 rows: (1,"Alice"), (2,"Bob"), (3,"Charlie").
     * To regenerate the resource, run this method and write the result to
     * libs/native/src/test/resources/org/elasticsearch/nativeaccess/test.parquet
     */
    static byte[] createTestParquetBytes() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("test_schema");

        return createParquetBytes(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("id", 1L);
            g1.add("name", "Alice");
            Group g2 = factory.newGroup();
            g2.add("id", 2L);
            g2.add("name", "Bob");
            Group g3 = factory.newGroup();
            g3.add("id", 3L);
            g3.add("name", "Charlie");
            return List.of(g1, g2, g3);
        });
    }

    @FunctionalInterface
    interface GroupCreator {
        List<Group> create(SimpleGroupFactory factory);
    }

    private Path writeSimpleParquetFile() throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");
        return writeTempParquet(schema, factory -> {
            Group g = factory.newGroup();
            g.add("id", 1L);
            return List.of(g);
        });
    }

    private Path writeTempParquet(MessageType schema, GroupCreator groupCreator) throws IOException {
        byte[] data = createParquetBytes(schema, groupCreator);
        Path dir = createTempDir();
        Path file = dir.resolve("test.parquet");
        Files.write(file, data);
        return file;
    }

    static byte[] createParquetBytes(MessageType schema, GroupCreator groupCreator) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(out);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        List<Group> groups = groupCreator.create(factory);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (Group group : groups) {
                writer.write(group);
            }
        }
        return out.toByteArray();
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
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
                        outputStream.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        outputStream.write(b, off, len);
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
