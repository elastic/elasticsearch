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

package org.elasticsearch.search.aggregations.metrics.scripted;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptMetrics;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalScriptedMetricTests extends InternalAggregationTestCase<InternalScriptedMetric> {

    private static final String REDUCE_SCRIPT_NAME = "reduceScript";
    // randomized only once so that any random test instance has the same value
    private boolean hasReduceScript = randomBoolean();

    @Override
    protected InternalScriptedMetric createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        Map<String, Object> params = new HashMap<>();
        if (randomBoolean()) {
            params.put(randomAsciiOfLength(5), randomAsciiOfLength(5));
        }
        Script reduceScript = null;
        if (hasReduceScript) {
            reduceScript = new Script(ScriptType.INLINE, MockScriptEngine.NAME, REDUCE_SCRIPT_NAME, params);
        }
        return new InternalScriptedMetric(name, randomAsciiOfLength(5), reduceScript, pipelineAggregators, metaData);
    }

    /**
     * Mock of the script service. The script that is run looks at the
     * "_aggs" parameter visible when executing the script and simply returns the count.
     * This should be equal to the number of input InternalScriptedMetrics that are reduced
     * in total.
     */
    @Override
    protected ScriptService mockScriptService() {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                // no file watching, so we don't need a ResourceWatcherService
                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), "false")
                .build();
        // mock script always retuns the size of the input aggs list as result
        @SuppressWarnings("unchecked")
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME,
                Collections.singletonMap(REDUCE_SCRIPT_NAME, script -> {
                    return ((List<Object>) script.get("_aggs")).size();
                }));
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singletonList(scriptEngine));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Collections.emptyList());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        try {
            return new ScriptService(settings, new Environment(settings), null, scriptEngineRegistry, scriptContextRegistry,
                    scriptSettings, new ScriptMetrics());
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    @Override
    protected void assertReduced(InternalScriptedMetric reduced, List<InternalScriptedMetric> inputs) {
        InternalScriptedMetric firstAgg = inputs.get(0);
        assertEquals(firstAgg.getName(), reduced.getName());
        assertEquals(firstAgg.pipelineAggregators(), reduced.pipelineAggregators());
        assertEquals(firstAgg.getMetaData(), reduced.getMetaData());
        if (hasReduceScript) {
            assertEquals(inputs.size(), reduced.aggregation());
        } else {
            assertEquals(inputs.size(), ((List<Object>) reduced.aggregation()).size());
        }
    }

    @Override
    protected Reader<InternalScriptedMetric> instanceReader() {
        return InternalScriptedMetric::new;
    }

}
