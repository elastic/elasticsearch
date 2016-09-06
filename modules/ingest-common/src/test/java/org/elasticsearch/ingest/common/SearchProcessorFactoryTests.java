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

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class SearchProcessorFactoryTests extends ESTestCase {

    private SearchProcessor.Factory factory;

    @Before
    public void createFactory() {
        factory = new SearchProcessor.Factory();
    }

    @After
    public void closeFactory() throws IOException {
        factory.close();
    }

    public void testRestClientInstances() throws Exception {
        // Create the first processor instance
        factory.create(null, randomAsciiOfLength(10), Collections.emptyMap());
        // We should have only one client (the default one)
        assertThat(factory.clients.size(), is(1));
        assertThat(factory.clients, hasKey("http://127.0.0.1:9200"));

        // Create the second processor instance
        factory.create(null, randomAsciiOfLength(10), Collections.emptyMap());
        // We should have only one client (the default one)
        assertThat(factory.clients.size(), is(1));

        // Create the third processor instance which defines another server
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "http://127.0.0.1:9250");
        factory.create(null, randomAsciiOfLength(10), configMap);
        // We should have two clients (the default one and the new one)
        assertThat(factory.clients.size(), is(2));
        assertThat(factory.clients, hasKey("http://127.0.0.1:9250"));
    }

    public void testFactoryIndexEmptyValidation() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("index", "");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, randomAsciiOfLength(10), configMap));

        assertThat(exception.getMessage(), is("[index] when set can't be empty"));
    }

    public void testFactoryTypeEmptyValidation() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("type", "");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, randomAsciiOfLength(10), configMap));

        assertThat(exception.getMessage(), is("[type] when set can't be empty"));
    }

    public void testFactoryIndexTypeValidation() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("type", "my_type");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, randomAsciiOfLength(10), configMap));

        assertThat(exception.getMessage(), is("[index/type] : if [type] is set, [index] must be set as well"));
    }
}
