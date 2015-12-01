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

package org.elasticsearch.search.aggregations.pipeline.having;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class BucketSelectorPipelineAggregator extends PipelineAggregator {

    public final static Type TYPE = new Type("bucket_selector");

    public final static PipelineAggregatorStreams.Stream STREAM = new PipelineAggregatorStreams.Stream() {
        @Override
        public BucketSelectorPipelineAggregator readResult(StreamInput in) throws IOException {
            BucketSelectorPipelineAggregator result = new BucketSelectorPipelineAggregator();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        PipelineAggregatorStreams.registerStream(STREAM, TYPE.stream());
    }

    private GapPolicy gapPolicy;

    private Script script;

    private Map<String, String> bucketsPathsMap;

    public BucketSelectorPipelineAggregator() {
    }

    public BucketSelectorPipelineAggregator(String name, Map<String, String> bucketsPathsMap, Script script, GapPolicy gapPolicy,
            Map<String, Object> metadata) {
        super(name, bucketsPathsMap.values().toArray(new String[bucketsPathsMap.size()]), metadata);
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
        this.gapPolicy = gapPolicy;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg = (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends Bucket> buckets = originalAgg.getBuckets();

        CompiledScript compiledScript = reduceContext.scriptService().compile(script, ScriptContext.Standard.AGGS, reduceContext);
        List newBuckets = new ArrayList<>();
        for (Bucket bucket : buckets) {
            Map<String, Object> vars = new HashMap<>();
            if (script.getParams() != null) {
                vars.putAll(script.getParams());
            }
            for (Map.Entry<String, String> entry : bucketsPathsMap.entrySet()) {
                String varName = entry.getKey();
                String bucketsPath = entry.getValue();
                Double value = resolveBucketValue(originalAgg, bucket, bucketsPath, gapPolicy);
                vars.put(varName, value);
            }
            ExecutableScript executableScript = reduceContext.scriptService().executable(compiledScript, vars);
            Object scriptReturnValue = executableScript.run();
            final boolean keepBucket;
            // TODO: WTF!!!!!
            if ("expression".equals(script.getLang())) {
                double scriptDoubleValue = (double) scriptReturnValue;
                keepBucket = scriptDoubleValue == 1.0;
            } else {
                keepBucket = (boolean) scriptReturnValue;
            }
            if (keepBucket) {
                newBuckets.add(bucket);
            }
        }
        return originalAgg.create(newBuckets);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        script.writeTo(out);
        gapPolicy.writeTo(out);
        out.writeGenericValue(bucketsPathsMap);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        script = Script.readScript(in);
        gapPolicy = GapPolicy.readFrom(in);
        bucketsPathsMap = (Map<String, String>) in.readGenericValue();
    }

    public static class Factory extends PipelineAggregatorFactory {

        private Script script;
        private GapPolicy gapPolicy = GapPolicy.SKIP;
        private Map<String, String> bucketsPathsMap;

        public Factory(String name, Map<String, String> bucketsPathsMap, Script script) {
            super(name, TYPE.name(), bucketsPathsMap.values().toArray(new String[bucketsPathsMap.size()]));
            this.bucketsPathsMap = bucketsPathsMap;
            this.script = script;
        }

        /**
         * Sets the gap policy to use for this aggregation.
         */
        public void gapPolicy(GapPolicy gapPolicy) {
            this.gapPolicy = gapPolicy;
        }

        /**
         * Gets the gap policy to use for this aggregation.
         */
        public GapPolicy gapPolicy() {
            return gapPolicy;
        }

        @Override
        protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
            return new BucketSelectorPipelineAggregator(name, bucketsPathsMap, script, gapPolicy, metaData);
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(BucketScriptParser.BUCKETS_PATH.getPreferredName(), bucketsPathsMap);
            builder.field(ScriptField.SCRIPT.getPreferredName(), script);
            builder.field(BucketScriptParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
            return builder;
        }

        @Override
        protected boolean overrideBucketsPath() {
            return true;
        }

        @Override
        protected PipelineAggregatorFactory doReadFrom(String name, String[] bucketsPaths, StreamInput in) throws IOException {
            Map<String, String> bucketsPathsMap = new HashMap<String, String>();
            int mapSize = in.readVInt();
            for (int i = 0; i < mapSize; i++) {
                bucketsPathsMap.put(in.readString(), in.readString());
            }
            Script script = Script.readScript(in);
            Factory factory = new Factory(name, bucketsPathsMap, script);
            factory.gapPolicy = GapPolicy.readFrom(in);
            return factory;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeVInt(bucketsPathsMap.size());
            for (Entry<String, String> e : bucketsPathsMap.entrySet()) {
                out.writeString(e.getKey());
                out.writeString(e.getValue());
            }
            script.writeTo(out);
            gapPolicy.writeTo(out);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(bucketsPathsMap, script, gapPolicy);
        }

        @Override
        protected boolean doEquals(Object obj) {
            Factory other = (Factory) obj;
            return Objects.equals(bucketsPathsMap, other.bucketsPathsMap) && Objects.equals(script, other.script)
                    && Objects.equals(gapPolicy, other.gapPolicy);
        }

    }

}
