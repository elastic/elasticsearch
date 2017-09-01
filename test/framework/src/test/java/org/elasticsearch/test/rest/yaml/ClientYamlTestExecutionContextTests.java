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

package org.elasticsearch.test.rest.yaml;

import org.apache.http.HttpEntity;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ClientYamlTestExecutionContextTests extends ESTestCase {

    public void testHeadersSupportStashedValueReplacement() throws IOException {
        final AtomicReference<Map<String, String>> headersRef = new AtomicReference<>();
        final ClientYamlTestExecutionContext context =
            new ClientYamlTestExecutionContext(null, randomBoolean()) {
                @Override
                ClientYamlTestResponse callApiInternal(String apiName, Map<String, String> params,
                                                       HttpEntity entity,
                                                       Map<String, String> headers) {
                    headersRef.set(headers);
                    return null;
                }
            };
        final Map<String, String> headers = new HashMap<>();
        headers.put("foo", "$bar");
        headers.put("foo1", "baz ${c}");

        context.stash().stashValue("bar", "foo2");
        context.stash().stashValue("c", "bar1");

        assertNull(headersRef.get());
        context.callApi("test", Collections.emptyMap(), Collections.emptyList(), headers);
        assertNotNull(headersRef.get());
        assertNotEquals(headers, headersRef.get());

        assertEquals("foo2", headersRef.get().get("foo"));
        assertEquals("baz bar1", headersRef.get().get("foo1"));
    }
}
