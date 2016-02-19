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

package org.elasticsearch.search.aggregations.pipeline.bucketscript;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

public class BucketScriptPipelineAggregatorBuilder extends PipelineAggregatorBuilder<BucketScriptPipelineAggregatorBuilder> {

    static final BucketScriptPipelineAggregatorBuilder PROTOTYPE = new BucketScriptPipelineAggregatorBuilder("", Collections.emptyMap(),
            new Script(""));

    private final Script script;
    private final Map<String, String> bucketsPathsMap;
    private String format = null;
    private GapPolicy gapPolicy = GapPolicy.SKIP;

    public BucketScriptPipelineAggregatorBuilder(String name, Map<String, String> bucketsPathsMap, Script script) {
        super(name, BucketScriptPipelineAggregator.TYPE.name(), bucketsPathsMap.values().toArray(new String[bucketsPathsMap.size()]));
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
    }

    public BucketScriptPipelineAggregatorBuilder(String name, Script script, String... bucketsPaths) {
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
     * Sets the format to use on the output of this aggregation.
     */
    public BucketScriptPipelineAggregatorBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
    }

    /**
     * Gets the format to use on the output of this aggregation.
     */
    public String format() {
        return format;
    }

    protected ValueFormatter formatter() {
        if (format != null) {
            return ValueFormat.Patternable.Number.format(format).formatter();
        } else {
            return ValueFormatter.RAW;
        }
    }

    /**
     * Sets the gap policy to use for this aggregation.
     */
    public BucketScriptPipelineAggregatorBuilder gapPolicy(GapPolicy gapPolicy) {
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
        return new BucketScriptPipelineAggregator(name, bucketsPathsMap, script, formatter(), gapPolicy, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(BucketScriptParser.BUCKETS_PATH.getPreferredName(), bucketsPathsMap);
        builder.field(ScriptField.SCRIPT.getPreferredName(), script);
        if (format != null) {
            builder.field(BucketScriptParser.FORMAT.getPreferredName(), format);
        }
        builder.field(BucketScriptParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        return builder;
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    protected BucketScriptPipelineAggregatorBuilder doReadFrom(String name, String[] bucketsPaths, StreamInput in) throws IOException {
        Map<String, String> bucketsPathsMap = new HashMap<String, String>();
        int mapSize = in.readVInt();
        for (int i = 0; i < mapSize; i++) {
            bucketsPathsMap.put(in.readString(), in.readString());
        }
        Script script = Script.readScript(in);
        BucketScriptPipelineAggregatorBuilder factory = new BucketScriptPipelineAggregatorBuilder(name, bucketsPathsMap, script);
        factory.format = in.readOptionalString();
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
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(bucketsPathsMap, script, format, gapPolicy);
    }

    @Override
    protected boolean doEquals(Object obj) {
        BucketScriptPipelineAggregatorBuilder other = (BucketScriptPipelineAggregatorBuilder) obj;
        return Objects.equals(bucketsPathsMap, other.bucketsPathsMap) && Objects.equals(script, other.script)
                && Objects.equals(format, other.format) && Objects.equals(gapPolicy, other.gapPolicy);
    }
}