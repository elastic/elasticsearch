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

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * This test verifies that the _aggs param is added correctly when the system property
 * "es.aggregations.enable_scripted_metric_agg_param" is set to true.
 */
public class InternalScriptedMetricAggStateV6CompatTests extends InternalAggregationTestCase<InternalScriptedMetric> {

    private static final String REDUCE_SCRIPT_NAME = "reduceScript";

    @Override
    protected InternalScriptedMetric createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        Script reduceScript =  new Script(ScriptType.INLINE, MockScriptEngine.NAME, REDUCE_SCRIPT_NAME, Collections.emptyMap());
        return new InternalScriptedMetric(name, "agg value", reduceScript, pipelineAggregators, metaData);
    }

    /**
     * Mock of the script service. The script that is run looks at the
     * "_aggs" parameter to verify that it was put in place by InternalScriptedMetric.
     */
    @Override
    protected ScriptService mockScriptService() {
        Function<Map<String, Object>, Object> script = vars -> {
            Object aggs = ((Map<String,Object>) vars.get("params")).get("_aggs");
            Object states = vars.get("states");
            assertThat(aggs, instanceOf(List.class));
            assertThat(aggs, sameInstance(states));
            return aggs;
        };

        @SuppressWarnings("unchecked")
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME,
                Collections.singletonMap(REDUCE_SCRIPT_NAME, script), Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    @Override
    protected void assertReduced(InternalScriptedMetric reduced, List<InternalScriptedMetric> inputs) {
        assertWarnings("Accessing variable [_aggs] via [params._aggs] from within a scripted metric agg reduce script " +
            "is deprecated in favor of using [state].");
    }

    @Override
    protected Reader<InternalScriptedMetric> instanceReader() {
        return InternalScriptedMetric::new;
    }

    @Override
    protected void assertFromXContent(InternalScriptedMetric aggregation, ParsedAggregation parsedAggregation) {}

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> path.contains(CommonFields.VALUE.getPreferredName());
    }

    @Override
    protected InternalScriptedMetric mutateInstance(InternalScriptedMetric instance) {
        String name = instance.getName();
        Object value = instance.aggregation();
        Script reduceScript = instance.reduceScript;
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        return new InternalScriptedMetric(name + randomAlphaOfLength(5), value, reduceScript, pipelineAggregators,
            metaData);
    }
}
