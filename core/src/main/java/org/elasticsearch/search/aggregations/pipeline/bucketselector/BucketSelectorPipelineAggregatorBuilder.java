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

package org.elasticsearch.search.aggregations.pipeline.bucketselector;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

public class BucketSelectorPipelineAggregatorBuilder extends PipelineAggregatorBuilder<BucketSelectorPipelineAggregatorBuilder> {

    static final BucketSelectorPipelineAggregatorBuilder PROTOTYPE = new BucketSelectorPipelineAggregatorBuilder("",
            Collections.emptyMap(), new Script(""));

    private Script script;
    private GapPolicy gapPolicy = GapPolicy.SKIP;
    private Map<String, String> bucketsPathsMap;

    public BucketSelectorPipelineAggregatorBuilder(String name, Map<String, String> bucketsPathsMap, Script script) {
        super(name, BucketSelectorPipelineAggregator.TYPE.name(), bucketsPathsMap.values().toArray(new String[bucketsPathsMap.size()]));
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
    }

    public BucketSelectorPipelineAggregatorBuilder(String name, Script script, String... bucketsPaths) {
        this(name, convertToBucketsPathMap(bucketsPaths), script);
    }

    private static Map<String, String> convertToBucketsPathMap(String[] bucketsPaths) {
        Map<String, String> bucketsPathsMap = new HashMap<>();
        for (int i = 0; i < bucketsPaths.length; i++) {
            bucketsPathsMap.put("_value" + i, bucketsPaths[i]);
        }
        return bucketsPathsMap;
    }

    /**
     * Sets the gap policy to use for this aggregation.
     */
    public BucketSelectorPipelineAggregatorBuilder gapPolicy(GapPolicy gapPolicy) {
        if (gapPolicy == null) {
            throw new IllegalArgumentException("[gapPolicy] must not be null: [" + name + "]");
        }
        this.gapPolicy = gapPolicy;
        return this;
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
    protected BucketSelectorPipelineAggregatorBuilder doReadFrom(String name, String[] bucketsPaths, StreamInput in)
            throws IOException {
        Map<String, String> bucketsPathsMap = new HashMap<String, String>();
        int mapSize = in.readVInt();
        for (int i = 0; i < mapSize; i++) {
            bucketsPathsMap.put(in.readString(), in.readString());
        }
        Script script = Script.readScript(in);
        BucketSelectorPipelineAggregatorBuilder factory = new BucketSelectorPipelineAggregatorBuilder(name, bucketsPathsMap, script);
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
        BucketSelectorPipelineAggregatorBuilder other = (BucketSelectorPipelineAggregatorBuilder) obj;
        return Objects.equals(bucketsPathsMap, other.bucketsPathsMap) && Objects.equals(script, other.script)
                && Objects.equals(gapPolicy, other.gapPolicy);
    }

}