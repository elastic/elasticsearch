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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class RestRequestTests extends ElasticsearchTestCase {

    @Test
    public void testContext() throws Exception {
        int count = randomInt(10);
        Request request = new Request();
        for (int i = 0; i < count; i++) {
            request.putInContext("key" + i, "val" + i);
        }
        assertThat(request.isContextEmpty(), is(count == 0));
        assertThat(request.contextSize(), is(count));
        ImmutableOpenMap<Object, Object> ctx = request.getContext();
        for (int i = 0; i < count; i++) {
            assertThat(request.hasInContext("key" + i), is(true));
            assertThat((String) request.getFromContext("key" + i), equalTo("val" + i));
            assertThat((String) ctx.get("key" + i), equalTo("val" + i));
        }
    }

    public static class Request extends RestRequest {
        @Override
        public Method method() {
            return null;
        }

        @Override
        public String uri() {
            return null;
        }

        @Override
        public String rawPath() {
            return null;
        }

        @Override
        public boolean hasContent() {
            return false;
        }

        @Override
        public BytesReference content() {
            return null;
        }

        @Override
        public String header(String name) {
            return null;
        }

        @Override
        public Iterable<Map.Entry<String, String>> headers() {
            return null;
        }

        @Override
        public boolean hasParam(String key) {
            return false;
        }

        @Override
        public String param(String key) {
            return null;
        }

        @Override
        public Map<String, String> params() {
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            return null;
        }
    }
}
