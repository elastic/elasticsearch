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

package org.elasticsearch.ingest;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class ForEachProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        ProcessorsRegistry.Builder builder = new ProcessorsRegistry.Builder();
        Processor processor = new TestProcessor(ingestDocument -> {});
        builder.registerProcessor("_name", (registry) -> config -> processor);
        ProcessorsRegistry registry = builder.build(mock(ScriptService.class), mock(ClusterService.class));
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(registry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processors", Collections.singletonList(Collections.singletonMap("_name", Collections.emptyMap())));
        ForEachProcessor forEachProcessor = forEachFactory.create(config);
        assertThat(forEachProcessor, Matchers.notNullValue());
        assertThat(forEachProcessor.getField(), Matchers.equalTo("_field"));
        assertThat(forEachProcessor.getProcessors().size(), Matchers.equalTo(1));
        assertThat(forEachProcessor.getProcessors().get(0), Matchers.sameInstance(processor));

        config = new HashMap<>();
        config.put("processors", Collections.singletonList(Collections.singletonMap("_name", Collections.emptyMap())));
        try {
            forEachFactory.create(config);
            fail("exception expected");
        } catch (Exception e) {
            assertThat(e.getMessage(), Matchers.equalTo("[field] required property is missing"));
        }

        config = new HashMap<>();
        config.put("field", "_field");
        try {
            forEachFactory.create(config);
            fail("exception expected");
        } catch (Exception e) {
            assertThat(e.getMessage(), Matchers.equalTo("[processors] required property is missing"));
        }
    }

}
