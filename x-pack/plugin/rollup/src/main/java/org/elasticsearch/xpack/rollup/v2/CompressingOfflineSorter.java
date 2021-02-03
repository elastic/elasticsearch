/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.OfflineSorter;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * An {@link OfflineSorter} that compresses the values using a {@link Deflater}.
 */
class CompressingOfflineSorter extends OfflineSorter {
    CompressingOfflineSorter(Directory dir,
                             String tempFileNamePrefix,
                             Comparator<BytesRef> comparator,
                             int ramBufferSizeMB) {
        super(dir, tempFileNamePrefix, comparator, OfflineSorter.BufferSize.megabytes(ramBufferSizeMB/2), 2, -1, null, 1);
    }

    static class Writer extends ByteSequencesWriter {
        final IndexOutput out;

        Writer(IndexOutput out) {
            super(out);
            this.out = out;
        }
    }

    @Override
    public ByteSequencesReader getReader(ChecksumIndexInput in, String name) throws IOException {
        // the footer is not compressed
        long gzipLen = in.length() - CodecUtil.footerLength();
        InflaterInputStream gzipInputStream = new InflaterInputStream(new InputStreamIndexInput(in, gzipLen));
        final DataInputStream dataIn = new DataInputStream(gzipInputStream);
        final BytesRefBuilder ref = new BytesRefBuilder();
        return new ByteSequencesReader(in, name) {

            public BytesRef next() throws IOException {
                short length = readShort();
                if (length == -1) {
                    return null;
                }
                ref.grow(length);
                ref.setLength(length);
                int readSize = 0;
                while (readSize < length) {
                    final int bytesRead = dataIn.read(ref.bytes(), readSize, length - readSize);
                    readSize += bytesRead;
                }
                return ref.get();
            }

            private short readShort() throws IOException {
                int ch1 = dataIn.read();
                if (ch1 == -1) {
                    return -1;
                }
                int ch2 = dataIn.read();
                short length = (short) ((ch1 << 8) + (ch2 << 0));
                assert length > 0;
                return length;
            }

            @Override
            public void close() throws IOException {
                IOUtils.close(dataIn);
                super.close();
            }
        };
    }

    @Override
    public Writer getWriter(IndexOutput out, long itemCount) throws IOException {
        final DeflaterOutputStream gzipOut = new DeflaterOutputStream(new IndexOutputOutputStream(out), true);
        final DataOutputStream dataOut = new DataOutputStream(gzipOut);
        // ensure that we flush the deflater when writing the footer
        return new Writer(new FlushIndexOutput(out.getName(), out, gzipOut)) {
            @Override
            public void write(byte[] bytes, int off, int len) throws IOException {
                assert bytes != null;
                assert off >= 0 && off + len <= bytes.length;
                assert len >= 0;
                if (len > Short.MAX_VALUE) {
                    throw new IllegalArgumentException("len must be <= " + Short.MAX_VALUE + "; got " + len);
                }
                dataOut.writeShort((short) len);
                dataOut.write(bytes, off, len);
            }

            @Override
            public void close() throws IOException {
                IOUtils.close(dataOut);
                super.close();
            }
        };
    }

    private static class FlushIndexOutput extends FilterIndexOutput {
        final DeflaterOutputStream gzip;
        boolean finished = false;

        private FlushIndexOutput(String resourceDescription, IndexOutput out, DeflaterOutputStream gzip) {
            super(resourceDescription, out);
            this.gzip = gzip;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            if (finished == false) {
                gzip.finish();
                finished = true;
            }
            out.writeByte(b);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            if (finished == false) {
                gzip.finish();
                finished = true;
            }
            out.writeBytes(b, offset, length);
        }
    }
}
