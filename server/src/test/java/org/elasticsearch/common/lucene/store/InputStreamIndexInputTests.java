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
}
