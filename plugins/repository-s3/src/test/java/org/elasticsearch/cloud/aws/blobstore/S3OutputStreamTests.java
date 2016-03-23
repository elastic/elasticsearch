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

package org.elasticsearch.cloud.aws.blobstore;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.common.io.Streams.copy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Unit test for {@link S3OutputStream}.
 */
public class S3OutputStreamTests extends ESTestCase {
    private static final int BUFFER_SIZE = new ByteSizeValue(5, ByteSizeUnit.MB).bytesAsInt();

    public void testWriteLessDataThanBufferSize() throws IOException {
        MockDefaultS3OutputStream out = newS3OutputStream(BUFFER_SIZE);
        byte[] content = randomUnicodeOfLengthBetween(1, 512).getBytes("UTF-8");
        copy(content, out);

        // Checks length & content
        assertThat(out.getLength(), equalTo((long) content.length));
        assertThat(Arrays.equals(content, out.toByteArray()), equalTo(true));

        // Checks single/multi part upload
        assertThat(out.getBufferSize(), equalTo(BUFFER_SIZE));
        assertThat(out.getFlushCount(), equalTo(1));
        assertThat(out.getNumberOfUploadRequests(), equalTo(1));
        assertFalse(out.isMultipart());

    }

    public void testWriteSameDataThanBufferSize() throws IOException {
        int size = randomIntBetween(BUFFER_SIZE, 2 * BUFFER_SIZE);
        MockDefaultS3OutputStream out = newS3OutputStream(size);

        ByteArrayOutputStream content = new ByteArrayOutputStream(size);
        for (int i = 0; i < size; i++) {
            content.write(randomByte());
        }
        copy(content.toByteArray(), out);

        // Checks length & content
        assertThat(out.getLength(), equalTo((long) size));
        assertThat(Arrays.equals(content.toByteArray(), out.toByteArray()), equalTo(true));

        // Checks single/multi part upload
        assertThat(out.getBufferSize(), equalTo(size));
        assertThat(out.getFlushCount(), equalTo(1));
        assertThat(out.getNumberOfUploadRequests(), equalTo(1));
        assertFalse(out.isMultipart());

    }

    public void testWriteExactlyNTimesMoreDataThanBufferSize() throws IOException {
        int n = randomIntBetween(2, 3);
        int length = n * BUFFER_SIZE;
        ByteArrayOutputStream content = new ByteArrayOutputStream(length);

        for (int i = 0; i < length; i++) {
            content.write(randomByte());
        }

        MockDefaultS3OutputStream out = newS3OutputStream(BUFFER_SIZE);
        copy(content.toByteArray(), out);

        // Checks length & content
        assertThat(out.getLength(), equalTo((long) length));
        assertThat(Arrays.equals(content.toByteArray(), out.toByteArray()), equalTo(true));

        // Checks single/multi part upload
        assertThat(out.getBufferSize(), equalTo(BUFFER_SIZE));
        assertThat(out.getFlushCount(), equalTo(n));

        assertThat(out.getNumberOfUploadRequests(), equalTo(n));
        assertTrue(out.isMultipart());
    }

    public void testWriteRandomNumberOfBytes() throws IOException {
        Integer randomBufferSize = randomIntBetween(BUFFER_SIZE, 2 * BUFFER_SIZE);
        MockDefaultS3OutputStream out = newS3OutputStream(randomBufferSize);

        Integer randomLength = randomIntBetween(1, 2 * BUFFER_SIZE);
        ByteArrayOutputStream content = new ByteArrayOutputStream(randomLength);
        for (int i = 0; i < randomLength; i++) {
            content.write(randomByte());
        }

        copy(content.toByteArray(), out);

        // Checks length & content
        assertThat(out.getLength(), equalTo((long) randomLength));
        assertThat(Arrays.equals(content.toByteArray(), out.toByteArray()), equalTo(true));

        assertThat(out.getBufferSize(), equalTo(randomBufferSize));
        int times = (int) Math.ceil(randomLength.doubleValue() / randomBufferSize.doubleValue());
        assertThat(out.getFlushCount(), equalTo(times));
        if (times > 1) {
            assertTrue(out.isMultipart());
        } else {
            assertFalse(out.isMultipart());
        }
    }

    public void testWrongBufferSize() throws IOException {
        Integer randomBufferSize = randomIntBetween(1, 4 * 1024 * 1024);
        try {
            newS3OutputStream(randomBufferSize);
            fail("Buffer size can't be smaller than 5mb");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("Buffer size can't be smaller than 5mb"));
        }
    }

    private MockDefaultS3OutputStream newS3OutputStream(int bufferSizeInBytes) {
        return new MockDefaultS3OutputStream(bufferSizeInBytes);
    }

}
