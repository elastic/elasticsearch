/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.common.lucene.store.DirectAccessIndexInput;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.simdvec.AbstractVectorTestCase;

import java.io.IOException;

/**
 * Checks which {@link IndexInput} types the scorer constructor accepts, i.e. its use of
 * {@link IndexInputUtils#checkInputType}: mmap, {@code DirectAccessInput} and plain inputs are all
 * fine, but a {@link FilterIndexInput} that hides them is rejected rather than silently losing
 * zero-copy access.
 */
public class MemorySegmentES92NativeInt7VectorsScorerTests extends AbstractVectorTestCase {

    private static final String FILE_NAME = "test.bin";

    public void testConstructorAcceptsPlainInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                new MemorySegmentES92NativeInt7VectorsScorer(in, 64, 16);
            }
        }
    }

    public void testConstructorAcceptsMMapInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                new MemorySegmentES92NativeInt7VectorsScorer(in, 64, 16);
            }
        }
    }

    public void testConstructorAcceptsDirectAccessInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput rawIn = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                IndexInput in = new DirectAccessIndexInput("dai", rawIn, data);
                new MemorySegmentES92NativeInt7VectorsScorer(in, 64, 16);
            }
        }
    }

    public void testConstructorRejectsUnwrappedFilterIndexInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput rawIn = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                IndexInput wrapped = new FilterIndexInput("plain-wrapper", rawIn) {};
                expectThrows(IllegalArgumentException.class, () -> new MemorySegmentES92NativeInt7VectorsScorer(wrapped, 64, 16));
            }
        }
    }

    private static void writeData(Directory dir, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(FILE_NAME, IOContext.DEFAULT)) {
            out.writeBytes(data, 0, data.length);
        }
    }
}
