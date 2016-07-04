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
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.mockito.Mockito.mock;

public class ProcessorsRegistryTests extends ESTestCase {

    public void testBuildProcessorRegistry() {
        ProcessorsRegistry.Builder builder = new ProcessorsRegistry.Builder();
        TestProcessor.Factory factory1 = new TestProcessor.Factory();
        builder.registerProcessor("1", (registry) -> factory1);
        TestProcessor.Factory factory2 = new TestProcessor.Factory();
        builder.registerProcessor("2", (registry) -> factory2);
        TestProcessor.Factory factory3 = new TestProcessor.Factory();
        try {
            builder.registerProcessor("1", (registry) -> factory3);
            fail("addProcessor should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Processor factory already registered for name [1]"));
        }

        ProcessorsRegistry registry = builder.build(mock(ScriptService.class), mock(ClusterService.class));
        assertThat(registry.getProcessorFactories().size(), equalTo(2));
        assertThat(registry.getProcessorFactory("1"), sameInstance(factory1));
        assertThat(registry.getProcessorFactory("2"), sameInstance(factory2));
    }
}
