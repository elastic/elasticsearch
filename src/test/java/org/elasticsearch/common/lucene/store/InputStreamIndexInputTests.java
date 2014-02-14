/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 *
 */
public class InputStreamIndexInputTests extends ElasticsearchTestCase {

    @Test
    public void testSingleReadSingleByteLimit() throws IOException {
        RAMDirectory dir = new RAMDirectory();
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
            assertThat(is.actualSizeToRead(), equalTo(1l));
            assertThat(is.read(), equalTo(1));
            assertThat(is.read(), equalTo(-1));
        }

        for (int i = 0; i < 3; i++) {
            InputStreamIndexInput is = new InputStreamIndexInput(input, 1);
            assertThat(input.getFilePointer(), lessThan(input.length()));
            assertThat(is.actualSizeToRead(), equalTo(1l));
            assertThat(is.read(), equalTo(2));
            assertThat(is.read(), equalTo(-1));
        }

        assertThat(input.getFilePointer(), equalTo(input.length()));
        InputStreamIndexInput is = new InputStreamIndexInput(input, 1);
        assertThat(is.actualSizeToRead(), equalTo(0l));
        assertThat(is.read(), equalTo(-1));
    }

    @Test
    public void testReadMultiSingleByteLimit1() throws IOException {
        RAMDirectory dir = new RAMDirectory();
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
            assertThat(is.actualSizeToRead(), equalTo(1l));
            assertThat(is.read(read), equalTo(1));
            assertThat(read[0], equalTo((byte) 1));
        }

        for (int i = 0; i < 3; i++) {
            assertThat(input.getFilePointer(), lessThan(input.length()));
            InputStreamIndexInput is = new InputStreamIndexInput(input, 1);
            assertThat(is.actualSizeToRead(), equalTo(1l));
            assertThat(is.read(read), equalTo(1));
            assertThat(read[0], equalTo((byte) 2));
        }

        assertThat(input.getFilePointer(), equalTo(input.length()));
        InputStreamIndexInput is = new InputStreamIndexInput(input, 1);
        assertThat(is.actualSizeToRead(), equalTo(0l));
        assertThat(is.read(read), equalTo(-1));
    }

    @Test
    public void testSingleReadTwoBytesLimit() throws IOException {
        RAMDirectory dir = new RAMDirectory();
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
        assertThat(is.actualSizeToRead(), equalTo(2l));
        assertThat(is.read(), equalTo(1));
        assertThat(is.read(), equalTo(1));
        assertThat(is.read(), equalTo(-1));

        assertThat(input.getFilePointer(), lessThan(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(2l));
        assertThat(is.read(), equalTo(1));
        assertThat(is.read(), equalTo(2));
        assertThat(is.read(), equalTo(-1));

        assertThat(input.getFilePointer(), lessThan(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(2l));
        assertThat(is.read(), equalTo(2));
        assertThat(is.read(), equalTo(2));
        assertThat(is.read(), equalTo(-1));

        assertThat(input.getFilePointer(), equalTo(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(0l));
        assertThat(is.read(), equalTo(-1));
    }

    @Test
    public void testReadMultiTwoBytesLimit1() throws IOException {
        RAMDirectory dir = new RAMDirectory();
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
        assertThat(is.actualSizeToRead(), equalTo(2l));
        assertThat(is.read(read), equalTo(2));
        assertThat(read[0], equalTo((byte) 1));
        assertThat(read[1], equalTo((byte) 1));

        assertThat(input.getFilePointer(), lessThan(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(2l));
        assertThat(is.read(read), equalTo(2));
        assertThat(read[0], equalTo((byte) 1));
        assertThat(read[1], equalTo((byte) 2));

        assertThat(input.getFilePointer(), lessThan(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(2l));
        assertThat(is.read(read), equalTo(2));
        assertThat(read[0], equalTo((byte) 2));
        assertThat(read[1], equalTo((byte) 2));

        assertThat(input.getFilePointer(), equalTo(input.length()));
        is = new InputStreamIndexInput(input, 2);
        assertThat(is.actualSizeToRead(), equalTo(0l));
        assertThat(is.read(read), equalTo(-1));
    }

    @Test
    public void testReadMultiFourBytesLimit() throws IOException {
        RAMDirectory dir = new RAMDirectory();
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
        assertThat(is.actualSizeToRead(), equalTo(4l));
        assertThat(is.read(read), equalTo(4));
        assertThat(read[0], equalTo((byte) 1));
        assertThat(read[1], equalTo((byte) 1));
        assertThat(read[2], equalTo((byte) 1));
        assertThat(read[3], equalTo((byte) 2));

        assertThat(input.getFilePointer(), lessThan(input.length()));
        is = new InputStreamIndexInput(input, 4);
        assertThat(is.actualSizeToRead(), equalTo(2l));
        assertThat(is.read(read), equalTo(2));
        assertThat(read[0], equalTo((byte) 2));
        assertThat(read[1], equalTo((byte) 2));

        assertThat(input.getFilePointer(), equalTo(input.length()));
        is = new InputStreamIndexInput(input, 4);
        assertThat(is.actualSizeToRead(), equalTo(0l));
        assertThat(is.read(read), equalTo(-1));
    }

    @Test
    public void testMarkRest() throws Exception {
        RAMDirectory dir = new RAMDirectory();
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
