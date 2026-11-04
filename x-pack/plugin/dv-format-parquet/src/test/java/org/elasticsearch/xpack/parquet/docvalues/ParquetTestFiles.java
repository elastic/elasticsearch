/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import io.airlift.compress.snappy.SnappyCompressor;

import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Writes Parquet files for tests without Hadoop, with the writer of parquet-mr and a codec factory that only knows
 * {@code UNCOMPRESSED}. The files have small row groups so that tests cross the borders of row groups.
 */
public final class ParquetTestFiles {

    /** A flat schema with required and optional columns of the types that the format serves, and a string. */
    public static final MessageType FLAT_SCHEMA = MessageTypeParser.parseMessageType(
        "message test { required int32 id; required int64 ts; required int32 low; required int64 bucket; optional int32 maybe; "
            + "required binary name (UTF8); required binary kind (UTF8); optional binary note (UTF8); }"
    );

    /**
     * How a file is written.
     *
     * @param dictionary whether columns use dictionary encoding
     * @param pageSize the target size of a data page in bytes, small to get many pages in a chunk
     * @param dictionaryPageSize the largest size of a dictionary page, small to make the writer fall back to {@code PLAIN} pages
     * @param codec {@code UNCOMPRESSED} or {@code SNAPPY}
     * @param v2 whether data pages have version 2. The writer then uses delta encodings for values that are not in a dictionary, which
     *           the reader does not support yet, so a file with version 2 pages may only have dictionary encoded columns
     */
    public record Options(boolean dictionary, int pageSize, int dictionaryPageSize, CompressionCodecName codec, boolean v2) {
        public static final Options DEFAULT = new Options(true, 1024, 1024 * 1024, CompressionCodecName.UNCOMPRESSED);

        public Options(boolean dictionary, int pageSize, int dictionaryPageSize, CompressionCodecName codec) {
            this(dictionary, pageSize, dictionaryPageSize, codec, false);
        }

        public Options withCodec(CompressionCodecName codec) {
            return new Options(dictionary, pageSize, dictionaryPageSize, codec, v2);
        }

        /** Data pages of version 2, which hold the levels apart from the values and compress only the values. */
        public Options withV2() {
            return new Options(dictionary, pageSize, dictionaryPageSize, codec, true);
        }
    }

    private ParquetTestFiles() {}

    /**
     * A directory for tests that attach stub segments. The test framework would run {@code CheckIndex} when it closes, which opens the
     * segments outside of the engine and reads every value, and that cannot work for a segment whose values are in external files.
     */
    public static Directory newStubDirectory() {
        BaseDirectoryWrapper directory = LuceneTestCase.newDirectory();
        directory.setCheckIndexOnClose(false);
        return directory;
    }

    /**
     * A file with the {@link #FLAT_SCHEMA}: {@code id} is the row number, {@code ts} is {@code id * 1000}, {@code low} is {@code id % 7}
     * and {@code bucket} is {@code (id % 11) * 10^10}, which have few distinct values and so stay dictionary encoded, {@code maybe} is
     * {@code id % 13} and missing for every third row, {@code name} is {@code "n" + id}, {@code kind} is {@code "k" + id % 5}, and
     * {@code note} is {@code "note-" + id % 9} and missing for every fourth row.
     *
     * @param rowGroupRows the number of rows of every row group except the last, which holds the rest
     */
    public static byte[] writeFlat(int rows, int rowGroupRows) throws IOException {
        return writeFlat(rows, rowGroupRows, Options.DEFAULT);
    }

    public static byte[] writeFlat(int rows, int rowGroupRows, Options options) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(FLAT_SCHEMA);
        try (ParquetWriter<Group> writer = writer(bytes, FLAT_SCHEMA, rowGroupRows, options)) {
            for (int i = 0; i < rows; i++) {
                Group group = factory.newGroup()
                    .append("id", i)
                    .append("ts", i * 1000L)
                    .append("low", i % 7)
                    .append("bucket", (i % 11) * 10_000_000_000L)
                    .append("name", "n" + i)
                    .append("kind", "k" + i % 5);
                if (i % 3 != 0) {
                    group.append("maybe", i % 13);
                }
                if (i % 4 != 0) {
                    group.append("note", "note-" + i % 9);
                }
                writer.write(group);
            }
        }
        return bytes.toByteArray();
    }

    /**
     * A file whose column {@code v} has few distinct values in the first half of the rows and only distinct values in the second half,
     * so that a writer with a small dictionary starts with dictionary pages and falls back to plain pages inside one chunk.
     */
    public static byte[] writeMixedCardinality(int rows, Options options) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message mixed { required int32 v; }");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = writer(bytes, schema, rows, options)) {
            for (int i = 0; i < rows; i++) {
                writer.write(factory.newGroup().append("v", mixedValue(i, rows)));
            }
        }
        return bytes.toByteArray();
    }

    /** The value of row {@code row} in {@link #writeMixedCardinality}. */
    public static int mixedValue(int row, int rows) {
        return row < rows / 2 ? row % 50 : row * 7;
    }

    /**
     * A file with a column of every type that the type table of the mapping knows, and some that it does not: {@code d} (double),
     * {@code f} (float), {@code flag} (boolean), {@code raw} (a byte array with no annotation), {@code day} (a {@code DATE}),
     * {@code ts_millis} and {@code ts_micros} (timestamps), {@code small} (a signed 16-bit integer) and {@code dec} (a decimal in a
     * byte array).
     */
    public static byte[] writeTypes(int rows) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message types { required double d; required float f; required boolean flag; required binary raw; required int32 day (DATE); "
                + "required int64 ts_millis (TIMESTAMP(MILLIS,true)); required int64 ts_micros (TIMESTAMP(MICROS,true)); "
                + "required int32 small (INT_16); required binary dec (DECIMAL(10,2)); }"
        );
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = writer(bytes, schema, rows, Options.DEFAULT)) {
            for (int i = 0; i < rows; i++) {
                writer.write(
                    factory.newGroup()
                        .append("d", i * 0.5)
                        .append("f", (float) i)
                        .append("flag", i % 2 == 0)
                        .append("raw", org.apache.parquet.io.api.Binary.fromString("r" + i))
                        .append("day", 19_000 + i)
                        .append("ts_millis", 1_700_000_000_000L + i)
                        .append("ts_micros", 1_700_000_000_000_000L + i)
                        .append("small", i)
                        .append("dec", org.apache.parquet.io.api.Binary.fromString("0"))
                );
            }
        }
        return bytes.toByteArray();
    }

    /**
     * A file with a required and an optional string column, for the values that tests choose.
     *
     * @param required the values of the required column
     * @param optional the values of the optional column, with {@code null} for a missing value
     */
    public static byte[] writeStrings(java.util.List<String> required, java.util.List<String> optional, Options options)
        throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message strings { required binary req (UTF8); optional binary opt (UTF8); }"
        );
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = writer(bytes, schema, Math.max(1, required.size() / 3), options)) {
            for (int i = 0; i < required.size(); i++) {
                Group group = factory.newGroup().append("req", required.get(i));
                if (optional.get(i) != null) {
                    group.append("opt", optional.get(i));
                }
                writer.write(group);
            }
        }
        return bytes.toByteArray();
    }

    /** A file with an unsigned 32-bit column that holds the values {@code 2^31 + row}, which do not fit a signed int. */
    public static byte[] writeUnsigned(int rows) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message unsigned { required int32 u (UINT_32); required int32 small (UINT_16); }"
        );
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = writer(bytes, schema, rows, Options.DEFAULT)) {
            for (int i = 0; i < rows; i++) {
                writer.write(factory.newGroup().append("u", Integer.MIN_VALUE + i).append("small", 65_000 + i));
            }
        }
        return bytes.toByteArray();
    }

    /** A file with the required columns {@code t} (an {@code int64}) and {@code u} (an {@code int32}) that hold the given values. */
    public static byte[] writeTimes(long[] t, int[] u, int rowGroupRows) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message times { required int64 t; required int32 u; }");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = writer(bytes, schema, rowGroupRows, Options.DEFAULT)) {
            for (int i = 0; i < t.length; i++) {
                writer.write(factory.newGroup().append("t", t[i]).append("u", u[i]));
            }
        }
        return bytes.toByteArray();
    }

    /** A file with the required columns {@code t} (an {@code int64}) and {@code s} (a UTF8 string) that hold the given values. */
    public static byte[] writeTimesAndStrings(long[] t, String[] s, int rowGroupRows) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message times { required int64 t; required binary s (UTF8); }");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = writer(bytes, schema, rowGroupRows, Options.DEFAULT)) {
            for (int i = 0; i < t.length; i++) {
                writer.write(factory.newGroup().append("t", t[i]).append("s", s[i]));
            }
        }
        return bytes.toByteArray();
    }

    /** A file whose only column is inside a struct. */
    public static byte[] writeNested(int rows) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message nested { required int32 id; required group inner { required int64 value; } }"
        );
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = writer(bytes, schema, rows, Options.DEFAULT)) {
            for (int i = 0; i < rows; i++) {
                Group group = factory.newGroup().append("id", i);
                group.addGroup("inner").append("value", (long) i);
                writer.write(group);
            }
        }
        return bytes.toByteArray();
    }

    private static ParquetWriter<Group> writer(ByteArrayOutputStream bytes, MessageType schema, int rowGroupRows, Options options)
        throws IOException {
        return ExampleParquetWriter.builder(new InMemoryOutputFile(bytes))
            .withConf(new PlainParquetConfiguration())
            .withCodecFactory(new TestCodecFactory())
            .withType(schema)
            .withCompressionCodec(options.codec())
            .withDictionaryEncoding(options.dictionary())
            .withDictionaryPageSize(options.dictionaryPageSize())
            .withRowGroupSize(1L << 30)
            .withRowGroupRowCountLimit(rowGroupRows)
            .withPageSize(options.pageSize())
            .withWriterVersion(options.v2() ? ParquetProperties.WriterVersion.PARQUET_2_0 : ParquetProperties.WriterVersion.PARQUET_1_0)
            .build();
    }

    private record InMemoryOutputFile(ByteArrayOutputStream bytes) implements OutputFile {
        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new PositionOutputStream() {
                private long position;

                @Override
                public long getPos() {
                    return position;
                }

                @Override
                public void write(int b) {
                    bytes.write(b);
                    position++;
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    bytes.write(b, off, len);
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
    }

    /** Knows {@code UNCOMPRESSED} and {@code SNAPPY}, which is what the tests need and needs no Hadoop. */
    private static final class TestCodecFactory implements CompressionCodecFactory {
        @Override
        public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
            if (codecName != CompressionCodecName.UNCOMPRESSED && codecName != CompressionCodecName.SNAPPY) {
                throw new UnsupportedOperationException("only UNCOMPRESSED and SNAPPY, not " + codecName);
            }
            return new BytesInputCompressor() {
                @Override
                public BytesInput compress(BytesInput bytes) throws IOException {
                    if (codecName == CompressionCodecName.UNCOMPRESSED) {
                        return bytes;
                    }
                    byte[] input = bytes.toByteArray();
                    SnappyCompressor compressor = new SnappyCompressor();
                    byte[] output = new byte[compressor.maxCompressedLength(input.length)];
                    int length = compressor.compress(input, 0, input.length, output, 0, output.length);
                    return BytesInput.from(output, 0, length);
                }

                @Override
                public CompressionCodecName getCodecName() {
                    return codecName;
                }

                @Override
                public void release() {}
            };
        }

        @Override
        public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
            throw new UnsupportedOperationException("the tests read with the code under test");
        }

        @Override
        public void release() {}
    }
}
