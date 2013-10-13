/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.io;

import com.google.common.base.Charsets;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;

import static org.elasticsearch.common.io.Streams.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link org.elasticsearch.common.io.Streams}.
 *
 *
 */
public class StreamsTests extends ElasticsearchTestCase {

    @Test
    public void testCopyFromInputStream() throws IOException {
        byte[] content = "content".getBytes(Charsets.UTF_8);
        ByteArrayInputStream in = new ByteArrayInputStream(content);
        ByteArrayOutputStream out = new ByteArrayOutputStream(content.length);
        long count = copy(in, out);

        assertThat(count, equalTo((long) content.length));
        assertThat(Arrays.equals(content, out.toByteArray()), equalTo(true));
    }

    @Test
    public void testCopyFromByteArray() throws IOException {
        byte[] content = "content".getBytes(Charsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream(content.length);
        copy(content, out);
        assertThat(Arrays.equals(content, out.toByteArray()), equalTo(true));
    }

    @Test
    public void testCopyToByteArray() throws IOException {
        byte[] content = "content".getBytes(Charsets.UTF_8);
        ByteArrayInputStream in = new ByteArrayInputStream(content);
        byte[] result = copyToByteArray(in);
        assertThat(Arrays.equals(content, result), equalTo(true));
    }

    @Test
    public void testCopyFromReader() throws IOException {
        String content = "content";
        StringReader in = new StringReader(content);
        StringWriter out = new StringWriter();
        int count = copy(in, out);
        assertThat(content.length(), equalTo(count));
        assertThat(out.toString(), equalTo(content));
    }

    @Test
    public void testCopyFromString() throws IOException {
        String content = "content";
        StringWriter out = new StringWriter();
        copy(content, out);
        assertThat(out.toString(), equalTo(content));
    }

    @Test
    public void testCopyToString() throws IOException {
        String content = "content";
        StringReader in = new StringReader(content);
        String result = copyToString(in);
        assertThat(result, equalTo(content));
    }

}
