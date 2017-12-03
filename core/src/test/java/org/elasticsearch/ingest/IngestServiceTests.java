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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

public class IngestServiceTests extends ESTestCase {
    private final IngestPlugin DUMMY_PLUGIN = new IngestPlugin() {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            return Collections.singletonMap("foo", (factories, tag, config) -> null);
        }
    };

    public void testIngestPlugin() {
        ThreadPool tp = Mockito.mock(ThreadPool.class);
        IngestService ingestService = new IngestService(Settings.EMPTY, tp, null, null,
            null, Collections.singletonList(DUMMY_PLUGIN));
        Map<String, Processor.Factory> factories = ingestService.getPipelineStore().getProcessorFactories();
        assertTrue(factories.containsKey("foo"));
        assertEquals(1, factories.size());
    }

    public void testIngestPluginDuplicate() {
        ThreadPool tp = Mockito.mock(ThreadPool.class);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new IngestService(Settings.EMPTY, tp, null, null,
            null, Arrays.asList(DUMMY_PLUGIN, DUMMY_PLUGIN)));
        assertTrue(e.getMessage(), e.getMessage().contains("already registered"));
    }
}
