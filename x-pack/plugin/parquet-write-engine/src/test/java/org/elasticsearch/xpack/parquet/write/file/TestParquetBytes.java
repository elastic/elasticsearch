/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

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
import org.elasticsearch.xpack.esql.datasource.parquet.PlainCompressionCodecFactory;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainParquetReadOptions;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Reads a Parquet file that is in memory, with no Hadoop. */
public final class TestParquetBytes {

    private TestParquetBytes() {}

    public record Contents(MessageType schema, List<Group> rows, int rowGroups) {}

    public static Contents read(byte[] bytes) throws IOException {
        final List<Group> rows = new ArrayList<>();
        int rowGroups = 0;
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new MemoryInputFile(bytes),
                PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
            )
        ) {
            final MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                rowGroups++;
                final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i = 0; i < pages.getRowCount(); i++) {
                    rows.add(recordReader.read());
                }
            }
            return new Contents(schema, rows, rowGroups);
        }
    }

    public record MemoryInputFile(byte[] bytes) implements InputFile {
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
                public void readFully(byte[] destination) throws IOException {
                    readFully(destination, 0, destination.length);
                }

                @Override
                public void readFully(byte[] destination, int start, int len) throws IOException {
                    if (buffer.remaining() < len) {
                        throw new EOFException();
                    }
                    buffer.get(destination, start, len);
                }

                @Override
                public int read(ByteBuffer destination) {
                    if (buffer.hasRemaining() == false) {
                        return -1;
                    }
                    final int length = Math.min(destination.remaining(), buffer.remaining());
                    destination.put(buffer.slice().limit(length));
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
