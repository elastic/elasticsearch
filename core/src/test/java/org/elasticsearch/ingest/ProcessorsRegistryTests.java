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

import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;

public class ProcessorsRegistryTests extends ESTestCase {

    public void testAddProcessor() {
        ProcessorsRegistry processorsRegistry = new ProcessorsRegistry();
        TestProcessor.Factory factory1 = new TestProcessor.Factory();
        processorsRegistry.registerProcessor("1", (templateService) -> factory1);
        TestProcessor.Factory factory2 = new TestProcessor.Factory();
        processorsRegistry.registerProcessor("2", (templateService) -> factory2);
        TestProcessor.Factory factory3 = new TestProcessor.Factory();
        try {
            processorsRegistry.registerProcessor("1", (templateService) -> factory3);
            fail("addProcessor should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Processor factory already registered for name [1]"));
        }

        Set<Map.Entry<String, Function<TemplateService, Processor.Factory<?>>>> entrySet = processorsRegistry.entrySet();
        assertThat(entrySet.size(), equalTo(2));
        for (Map.Entry<String, Function<TemplateService, Processor.Factory<?>>> entry : entrySet) {
            if (entry.getKey().equals("1")) {
                assertThat(entry.getValue().apply(null), equalTo(factory1));
            } else if (entry.getKey().equals("2")) {
                assertThat(entry.getValue().apply(null), equalTo(factory2));
            } else {
                fail("unexpected processor id [" + entry.getKey() + "]");
            }
        }
    }
}
