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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ScriptedMetricAggregator extends MetricsAggregator {

    private final SearchScript mapScript;
    private final ExecutableScript combineScript;
    private final Script reduceScript;
    private Map<String, Object> params;

    protected ScriptedMetricAggregator(String name, Script initScript, Script mapScript, Script combineScript, Script reduceScript,
            Map<String, Object> params, AggregationContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.params = params;
        ScriptService scriptService = context.searchContext().scriptService();
        if (initScript != null) {
            scriptService.executable(initScript, ScriptContext.Standard.AGGS, Collections.emptyMap()).run();
        }
        this.mapScript = scriptService.search(context.searchContext().lookup(), mapScript, ScriptContext.Standard.AGGS, Collections.emptyMap());
        if (combineScript != null) {
            this.combineScript = scriptService.executable(combineScript, ScriptContext.Standard.AGGS, Collections.emptyMap());
        } else {
            this.combineScript = null;
        }
        this.reduceScript = reduceScript;
    }

    @Override
    public boolean needsScores() {
        return true; // TODO: how can we know if the script relies on scores?
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final LeafSearchScript leafMapScript = mapScript.getLeafSearchScript(ctx);
        return new LeafBucketCollectorBase(sub, mapScript) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0 : bucket;
                leafMapScript.setDocument(doc);
                leafMapScript.run();
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        Object aggregation;
        if (combineScript != null) {
            aggregation = combineScript.run();
        } else {
            aggregation = params.get("_agg");
        }
        return new InternalScriptedMetric(name, aggregation, reduceScript, pipelineAggregators(),
                metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalScriptedMetric(name, null, reduceScript, pipelineAggregators(), metaData());
    }

    public static class ScriptedMetricAggregatorBuilder extends AggregatorBuilder<ScriptedMetricAggregatorBuilder> {

        private Script initScript;
        private Script mapScript;
        private Script combineScript;
        private Script reduceScript;
        private Map<String, Object> params;

        public ScriptedMetricAggregatorBuilder(String name) {
            super(name, InternalScriptedMetric.TYPE);
        }

        /**
         * Set the <tt>init</tt> script.
         */
        public ScriptedMetricAggregatorBuilder initScript(Script initScript) {
            this.initScript = initScript;
            return this;
        }

        /**
         * Get the <tt>init</tt> script.
         */
        public Script initScript() {
            return initScript;
        }

        /**
         * Set the <tt>map</tt> script.
         */
        public ScriptedMetricAggregatorBuilder mapScript(Script mapScript) {
            this.mapScript = mapScript;
            return this;
        }

        /**
         * Get the <tt>map</tt> script.
         */
        public Script mapScript() {
            return mapScript;
        }

        /**
         * Set the <tt>combine</tt> script.
         */
        public ScriptedMetricAggregatorBuilder combineScript(Script combineScript) {
            this.combineScript = combineScript;
            return this;
        }

        /**
         * Get the <tt>combine</tt> script.
         */
        public Script combineScript() {
            return combineScript;
        }

        /**
         * Set the <tt>reduce</tt> script.
         */
        public ScriptedMetricAggregatorBuilder reduceScript(Script reduceScript) {
            this.reduceScript = reduceScript;
            return this;
        }

        /**
         * Get the <tt>reduce</tt> script.
         */
        public Script reduceScript() {
            return reduceScript;
        }

        /**
         * Set parameters that will be available in the <tt>init</tt>,
         * <tt>map</tt> and <tt>combine</tt> phases.
         */
        public ScriptedMetricAggregatorBuilder params(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        /**
         * Get parameters that will be available in the <tt>init</tt>,
         * <tt>map</tt> and <tt>combine</tt> phases.
         */
        public Map<String, Object> params() {
            return params;
        }

        @Override
        protected AggregatorFactory<?> doBuild(AggregationContext context) {
            return new ScriptedMetricAggregatorFactory(name, type, initScript, mapScript, combineScript, reduceScript, params);
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params builderParams) throws IOException {
            builder.startObject();
            if (initScript != null) {
                builder.field(ScriptedMetricParser.INIT_SCRIPT_FIELD.getPreferredName(), initScript);
            }

            if (mapScript != null) {
                builder.field(ScriptedMetricParser.MAP_SCRIPT_FIELD.getPreferredName(), mapScript);
            }

            if (combineScript != null) {
                builder.field(ScriptedMetricParser.COMBINE_SCRIPT_FIELD.getPreferredName(), combineScript);
            }

            if (reduceScript != null) {
                builder.field(ScriptedMetricParser.REDUCE_SCRIPT_FIELD.getPreferredName(), reduceScript);
            }
            if (params != null) {
                builder.field(ScriptedMetricParser.PARAMS_FIELD.getPreferredName());
                builder.map(params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        protected ScriptedMetricAggregatorBuilder doReadFrom(String name, StreamInput in) throws IOException {
            ScriptedMetricAggregatorBuilder factory = new ScriptedMetricAggregatorBuilder(name);
            factory.initScript = in.readOptionalStreamable(Script.SUPPLIER);
            factory.mapScript = in.readOptionalStreamable(Script.SUPPLIER);
            factory.combineScript = in.readOptionalStreamable(Script.SUPPLIER);
            factory.reduceScript = in.readOptionalStreamable(Script.SUPPLIER);
            if (in.readBoolean()) {
                factory.params = in.readMap();
            }
            return factory;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeOptionalStreamable(initScript);
            out.writeOptionalStreamable(mapScript);
            out.writeOptionalStreamable(combineScript);
            out.writeOptionalStreamable(reduceScript);
            boolean hasParams = params != null;
            out.writeBoolean(hasParams);
            if (hasParams) {
                out.writeMap(params);
            }
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(initScript, mapScript, combineScript, reduceScript, params);
        }

        @Override
        protected boolean doEquals(Object obj) {
            ScriptedMetricAggregatorBuilder other = (ScriptedMetricAggregatorBuilder) obj;
            return Objects.equals(initScript, other.initScript)
                    && Objects.equals(mapScript, other.mapScript)
                    && Objects.equals(combineScript, other.combineScript)
                    && Objects.equals(reduceScript, other.reduceScript)
                    && Objects.equals(params, other.params);
        }

    }

}
