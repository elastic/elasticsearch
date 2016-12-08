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

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class RestRequestTests extends ESTestCase {
    public void testContentOrSourceParamParserOrNull() throws IOException {
        assertNull(new ContentRestRequest("", emptyMap()).contentOrSourceParamParserOrNull());
        assertEquals(emptyMap(), new ContentRestRequest("{}", emptyMap()).contentOrSourceParamParserOrNull().map());
        assertEquals(emptyMap(), new ContentRestRequest("", singletonMap("source", "{}")).contentOrSourceParamParserOrNull().map());
    }

    public void testContentOrSourceParamParser() throws IOException {
        Exception e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", emptyMap()).contentOrSourceParamParser());
        assertEquals("Body required", e.getMessage());
        assertEquals(emptyMap(), new ContentRestRequest("{}", emptyMap()).contentOrSourceParamParser().map());
        assertEquals(emptyMap(), new ContentRestRequest("", singletonMap("source", "{}")).contentOrSourceParamParser().map());
    }

    private static final class ContentRestRequest extends RestRequest {
        private final BytesArray content;
        public ContentRestRequest(String content, Map<String, String> params) {
            super(params, "not used by this test");
            this.content = new BytesArray(content);
        }

        @Override
        public boolean hasContent() {
            return Strings.hasLength(content);
        }
        
        @Override
        public BytesReference content() {
            return content;
        }

        @Override
        public String uri() {
            throw new UnsupportedOperationException("Not used by this test");
        }

        @Override
        public Method method() {
            throw new UnsupportedOperationException("Not used by this test");
        }

        @Override
        public Iterable<Entry<String, String>> headers() {
            throw new UnsupportedOperationException("Not used by this test");
        }

        @Override
        public String header(String name) {
            throw new UnsupportedOperationException("Not used by this test");
        }
    }
}
