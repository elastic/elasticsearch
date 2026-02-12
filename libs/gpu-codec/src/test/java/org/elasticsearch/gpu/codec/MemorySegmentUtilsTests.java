/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("*") // we do our own mocking
public class MemorySegmentUtilsTests extends ESTestCase {

    private FSDirectory newRandomDirectory() throws IOException {
        return randomBoolean() ? new MMapDirectory(createTempDir()) : new NIOFSDirectory(createTempDir());
    }

    public void testCopyInputToTempFile() throws IOException {
        int dataSize = randomIntBetween(1000, 2 * 1024 * 1024);
        var data = randomByteArrayOfLength(dataSize);

        try (FSDirectory dir = newRandomDirectory()) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                out.writeBytes(data, 0, dataSize);
            }

            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                if (rarely()) {
                    in.seek(randomIntBetween(0, dataSize - 1));
                }

                var initialPosition = in.getFilePointer();

                var tempFilePath = MemorySegmentUtils.copyInputToTempFile(in, dir, "tests.bin");

                assertThat(
                    tempFilePath.toString(),
                    startsWith(dir.getDirectory() + tempFilePath.getFileSystem().getSeparator() + "tests.bin")
                );

                assertThat(initialPosition, equalTo(in.getFilePointer()));
                assertArrayEquals(data, Files.readAllBytes(tempFilePath));
            }
        }
    }

    public void testCopyInputToTempFilePacked() throws IOException {
        int rowSize = randomIntBetween(1, 2000);
        int paddingSize = randomIntBetween(0, 20);
        int rows = randomIntBetween(1, 100);

        var data = randomByteArrayOfLength(rowSize * rows);
        var scratch = new byte[paddingSize];

        try (FSDirectory dir = newRandomDirectory()) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < rows; ++i) {
                    out.writeBytes(data, i * rowSize, rowSize);
                    randomBytesBetween(scratch, (byte) 0, (byte) 127);
                    out.writeBytes(scratch, 0, paddingSize);
                }
            }

            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                if (rarely()) {
                    in.seek(randomIntBetween(0, data.length - 1));
                }

                var initialPosition = in.getFilePointer();

                var tempFilePath = MemorySegmentUtils.copyInputToTempFilePacked(in, dir, "tests.bin", rows, rowSize + paddingSize, rowSize);

                assertThat(
                    tempFilePath.toString(),
                    startsWith(dir.getDirectory() + tempFilePath.getFileSystem().getSeparator() + "tests.bin")
                );

                assertThat(initialPosition, equalTo(in.getFilePointer()));
                assertArrayEquals(data, Files.readAllBytes(tempFilePath));
            }
        }
    }

    public void testCreateFileBackedMemorySegment() throws IOException {
        var tempFile = createTempFile("test", "bin");
        var data = randomByteArrayOfLength(randomIntBetween(10, 1024 * 1024));
        Files.write(tempFile, data);

        try (var holder = MemorySegmentUtils.createFileBackedMemorySegment(tempFile, data.length)) {
            assertThat(holder.memorySegment().address(), is(not(0)));
            assertArrayEquals(data, holder.memorySegment().toArray(ValueLayout.JAVA_BYTE));
        }
    }

    public void testGetContiguousMemorySegmentBelowMaxChunkSize() throws IOException {
        var maxChunkSize = 2 * 1024 * 1024;
        int dataSize = randomIntBetween(1000, maxChunkSize - 1);
        var data = randomByteArrayOfLength(dataSize);

        try (FSDirectory dir = new MMapDirectory(createTempDir(), maxChunkSize)) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                out.writeBytes(data, 0, dataSize);
            }

            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {

                var msai = (MemorySegmentAccessInput) in;
                var holder = MemorySegmentUtils.getContiguousMemorySegment(msai, dir, "tests.bin");

                assertThat(holder, isA(MemorySegmentUtils.DirectMemorySegmentHolder.class));
                assertNotNull(holder.memorySegment());
                assertThat(holder.memorySegment().address(), is(not(0)));
                assertArrayEquals(data, holder.memorySegment().toArray(ValueLayout.JAVA_BYTE));
            }
        }
    }

    public void testGetContiguousMemorySegmentAboveMaxChunkSize() throws IOException {
        var maxChunkSize = 1000;
        int dataSize = randomIntBetween(1001, 3000);
        var data = randomByteArrayOfLength(dataSize);

        try (FSDirectory dir = new MMapDirectory(createTempDir(), maxChunkSize)) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                out.writeBytes(data, 0, dataSize);
            }

            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {

                var msai = (MemorySegmentAccessInput) in;
                var holder = MemorySegmentUtils.getContiguousMemorySegment(msai, dir, "tests.bin");

                assertThat(holder, isA(MemorySegmentUtils.FileBackedMemorySegmentHolder.class));
                assertNotNull(holder.memorySegment());
                assertThat(holder.memorySegment().address(), is(not(0)));
                assertArrayEquals(data, holder.memorySegment().toArray(ValueLayout.JAVA_BYTE));
            }
        }
    }
}
