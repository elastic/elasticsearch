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
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class RestRequestTests extends ESTestCase {
    public void testContentParser() throws IOException {
        Exception e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", emptyMap()).contentParser());
        assertEquals("Body required", e.getMessage());
        e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", singletonMap("source", "{}")).contentParser());
        assertEquals("Body required", e.getMessage());
        assertEquals(emptyMap(), new ContentRestRequest("{}", emptyMap()).contentParser().map());
    }

    public void testApplyContentParser() throws IOException {
        new ContentRestRequest("", emptyMap()).applyContentParser(p -> fail("Shouldn't have been called"));
        new ContentRestRequest("", singletonMap("source", "{}")).applyContentParser(p -> fail("Shouldn't have been called"));
        AtomicReference<Object> source = new AtomicReference<>();
        new ContentRestRequest("{}", emptyMap()).applyContentParser(p -> source.set(p.map()));
        assertEquals(emptyMap(), source.get());
    }

    public void testContentOrSourceParam() throws IOException {
        assertEquals(BytesArray.EMPTY, new ContentRestRequest("", emptyMap()).contentOrSourceParam());
        assertEquals(new BytesArray("stuff"), new ContentRestRequest("stuff", emptyMap()).contentOrSourceParam());
        assertEquals(new BytesArray("stuff"), new ContentRestRequest("stuff", singletonMap("source", "stuff2")).contentOrSourceParam());
        assertEquals(new BytesArray("stuff"), new ContentRestRequest("", singletonMap("source", "stuff")).contentOrSourceParam());
    }

    public void testHasContentOrSourceParam() throws IOException {
        assertEquals(false, new ContentRestRequest("", emptyMap()).hasContentOrSourceParam());
        assertEquals(true, new ContentRestRequest("stuff", emptyMap()).hasContentOrSourceParam());
        assertEquals(true, new ContentRestRequest("stuff", singletonMap("source", "stuff2")).hasContentOrSourceParam());
        assertEquals(true, new ContentRestRequest("", singletonMap("source", "stuff")).hasContentOrSourceParam());
    }

    public void testContentOrSourceParamParser() throws IOException {
        Exception e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", emptyMap()).contentOrSourceParamParser());
        assertEquals("Body required", e.getMessage());
        assertEquals(emptyMap(), new ContentRestRequest("{}", emptyMap()).contentOrSourceParamParser().map());
        assertEquals(emptyMap(), new ContentRestRequest("{}", singletonMap("source", "stuff2")).contentOrSourceParamParser().map());
        assertEquals(emptyMap(), new ContentRestRequest("", singletonMap("source", "{}")).contentOrSourceParamParser().map());
    }

    public void testWithContentOrSourceParamParserOrNull() throws IOException {
        new ContentRestRequest("", emptyMap()).withContentOrSourceParamParserOrNull(parser -> assertNull(parser));
        new ContentRestRequest("{}", emptyMap()).withContentOrSourceParamParserOrNull(parser -> assertEquals(emptyMap(), parser.map()));
        new ContentRestRequest("{}", singletonMap("source", "stuff2")).withContentOrSourceParamParserOrNull(parser ->
                assertEquals(emptyMap(), parser.map()));
        new ContentRestRequest("", singletonMap("source", "{}")).withContentOrSourceParamParserOrNull(parser ->
                assertEquals(emptyMap(), parser.map()));
    }

    private static final class ContentRestRequest extends RestRequest {
        private final BytesArray content;
        public ContentRestRequest(String content, Map<String, String> params) {
            super(NamedXContentRegistry.EMPTY, params, "not used by this test");
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
