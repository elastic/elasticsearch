/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class InputStreamIndexInputTests extends ESTestCase {
    public void testSingleReadSingleByteLimit() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 1);
        }
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 2);
        }

        output.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);

        for (int i = 0; i < 3; i++) {
            InputStreamIndexInput is = new InputStreamIndexInput(input, 1);
            assertThat(input.getFilePointer(), lessThan(input.length()));
            assertThat(is.actualSizeToRead(), equalTo(1L));
            assertThat(is.read(), equalTo(1));
            assertThat(is.read(), equalTo(-1));
        }

        for (int i = 0; i < 3; i++) {
            InputStreamIndexInput is = new InputStreamIndexInput(input, 1);
            assertThat(input.getFilePointer(), lessThan(input.length()));
            assertThat(is.actualSizeToRead(), equalTo(1L));
            assertThat(is.read(), equalTo(2));
            assertThat(is.read(), equalTo(-1));
        }

        assertThat(input.getFilePointer(), equalTo(input.length()));
        InputStreamIndexInput is = new InputStreamIndexInput(input, 1);
        assertThat(is.actualSizeToRead(), equalTo(0L));
        assertThat(is.read(), equalTo(-1));
    }

    public void testReadMultiSingleByteLimit1() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 1);
        }
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 2);
        }

        output.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);

        byte[] read = new byte[2];

        for (int i = 0; i < 3; i++) {
            assertThat(input.getFilePointer(), lessThan(input.length()));
            InputStreamIndexInput is = new InputStreamIndexInput(input, 1);
            assertThat(is.actualSizeToRead(), equalTo(1L));
            assertThat(is.read(read), equalTo(1));
            assertThat(read[0], equalTo((byte) 1));
        }

        for (int i = 0; i < 3; i++) {
            assertThat(input.getFilePointer(), lessThan(input.length()));
            InputStreamIndexInput is = new InputStreamIndexInput(input, 1);
            assertThat(is.actualSizeToRead(), equalTo(1L));
            assertThat(is.read(read), equalTo(1));
            assertThat(read[0], equalTo((byte) 2));
        }

        assertThat(input.getFilePointer(), equalTo(input.length()));
        InputStreamIndexInput is = new InputStreamIndexInput(input, 1);
        assertThat(is.actualSizeToRead(), equalTo(0L));
        assertThat(is.read(read), equalTo(-1));
    }

    public void testSingleReadTwoBytesLimit() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 1);
        }
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 2);
        }

        output.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);

        assertThat(input.getFilePointer(), lessThan(input.length()));
        InputStreamIndexInput is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(2L));
        assertThat(is.read(), equalTo(1));
        assertThat(is.read(), equalTo(1));
        assertThat(is.read(), equalTo(-1));

        assertThat(input.getFilePointer(), lessThan(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(2L));
        assertThat(is.read(), equalTo(1));
        assertThat(is.read(), equalTo(2));
        assertThat(is.read(), equalTo(-1));

        assertThat(input.getFilePointer(), lessThan(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(2L));
        assertThat(is.read(), equalTo(2));
        assertThat(is.read(), equalTo(2));
        assertThat(is.read(), equalTo(-1));

        assertThat(input.getFilePointer(), equalTo(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(0L));
        assertThat(is.read(), equalTo(-1));
    }

    public void testReadMultiTwoBytesLimit1() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 1);
        }
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 2);
        }

        output.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);

        byte[] read = new byte[2];

        assertThat(input.getFilePointer(), lessThan(input.length()));
        InputStreamIndexInput is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(2L));
        assertThat(is.read(read), equalTo(2));
        assertThat(read[0], equalTo((byte) 1));
        assertThat(read[1], equalTo((byte) 1));

        assertThat(input.getFilePointer(), lessThan(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(2L));
        assertThat(is.read(read), equalTo(2));
        assertThat(read[0], equalTo((byte) 1));
        assertThat(read[1], equalTo((byte) 2));

        assertThat(input.getFilePointer(), lessThan(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(2L));
        assertThat(is.read(read), equalTo(2));
        assertThat(read[0], equalTo((byte) 2));
        assertThat(read[1], equalTo((byte) 2));

        assertThat(input.getFilePointer(), equalTo(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(0L));
        assertThat(is.read(read), equalTo(-1));
    }

    public void testReadMultiFourBytesLimit() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 1);
        }
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 2);
        }

        output.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);

        byte[] read = new byte[4];

        assertThat(input.getFilePointer(), lessThan(input.length()));
        InputStreamIndexInput is = new InputStreamIndexInput(input, 4);
        assertThat(is.actualSizeToRead(), equalTo(4L));
        assertThat(is.read(read), equalTo(4));
        assertThat(read[0], equalTo((byte) 1));
        assertThat(read[1], equalTo((byte) 1));
        assertThat(read[2], equalTo((byte) 1));
        assertThat(read[3], equalTo((byte) 2));

        assertThat(input.getFilePointer(), lessThan(input.length()));
        is = new InputStreamIndexInput(input, 4);
        assertThat(is.actualSizeToRead(), equalTo(2L));
        assertThat(is.read(read), equalTo(2));
        assertThat(read[0], equalTo((byte) 2));
        assertThat(read[1], equalTo((byte) 2));

        assertThat(input.getFilePointer(), equalTo(input.length()));
        is = new InputStreamIndexInput(input, 4);
        assertThat(is.actualSizeToRead(), equalTo(0L));
        assertThat(is.read(read), equalTo(-1));
    }

    public void testMarkRest() throws Exception {
        Directory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 1);
        }
        for (int i = 0; i < 3; i++) {
            output.writeByte((byte) 2);
        }

        output.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);
        InputStreamIndexInput is = new InputStreamIndexInput(input, 4);
        assertThat(is.markSupported(), equalTo(true));
        assertThat(is.read(), equalTo(1));
        assertThat(is.read(), equalTo(1));
        is.mark(0);
        assertThat(is.read(), equalTo(1));
        assertThat(is.read(), equalTo(2));
        is.reset();
        assertThat(is.read(), equalTo(1));
        assertThat(is.read(), equalTo(2));
    }

    public void testReadZeroShouldReturnZero() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexOutput output = dir.createOutput("test", IOContext.DEFAULT)) {
                output.writeByte((byte) 1);
            }
            try (IndexInput input = dir.openInput("test", IOContext.DEFAULT)) {
                assertEquals(0, new InputStreamIndexInput(input, input.length()).read(new byte[randomIntBetween(0, 16)], 0, 0));
            }
        }
    }

    public void testReadAllBytes() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            // Need to be bigger than InputStream#DEFAULT_BUFFER_SIZE in order to test that `readAllBytes`
            // will call `read` again after calling it with length 0
            int size = randomIntBetween(8193, 10_000);
            try (IndexOutput output = dir.createOutput("test", IOContext.DEFAULT)) {
                output.writeBytes(randomByteArrayOfLength(size), size);
            }
            try (IndexInput input = dir.openInput("test", IOContext.DEFAULT)) {
                byte[] bytes = new InputStreamIndexInput(input, input.length()).readAllBytes();
                assertEquals(size, bytes.length);
            }
            try (IndexInput input = dir.openInput("test", IOContext.DEFAULT)) {
                // Verify that the respect the limit condition
                long limit = randomLongBetween(0, input.length());
                byte[] bytes = new InputStreamIndexInput(input, limit).readAllBytes();
                assertEquals(limit, bytes.length);
            }
        }
    }
}
