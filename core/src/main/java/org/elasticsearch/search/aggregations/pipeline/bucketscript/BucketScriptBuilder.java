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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;

import java.io.IOException;
import java.util.Map;

public class BucketScriptBuilder extends PipelineAggregatorBuilder<BucketScriptBuilder> {

    private String format;
    private GapPolicy gapPolicy;
    private Script script;
    private Map<String, String> bucketsPathsMap;

    public BucketScriptBuilder(String name) {
        super(name, BucketScriptPipelineAggregator.TYPE.name());
    }

    public BucketScriptBuilder script(Script script) {
        this.script = script;
        return this;
    }

    public BucketScriptBuilder format(String format) {
        this.format = format;
        return this;
    }

    public BucketScriptBuilder gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
        return this;
    }

    /**
     * Sets the paths to the buckets to use for this pipeline aggregator
     */
    public BucketScriptBuilder setBucketsPathsMap(Map<String, String> bucketsPathsMap) {
        this.bucketsPathsMap = bucketsPathsMap;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params builderParams) throws IOException {
        if (script != null) {
            builder.field(ScriptField.SCRIPT.getPreferredName(), script);
        }
        if (format != null) {
            builder.field(BucketScriptParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(BucketScriptParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        if (bucketsPathsMap != null) {
            builder.field(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName(), bucketsPathsMap);
        }
        return builder;
    }

}
