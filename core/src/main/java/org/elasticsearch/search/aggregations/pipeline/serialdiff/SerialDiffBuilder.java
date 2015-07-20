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

package org.elasticsearch.search.aggregations.pipeline.serialdiff;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;

import java.io.IOException;

public class SerialDiffBuilder extends PipelineAggregatorBuilder<SerialDiffBuilder> {

    private String format;
    private GapPolicy gapPolicy;
    private Integer lag;

    public SerialDiffBuilder(String name) {
        super(name, SerialDiffPipelineAggregator.TYPE.name());
    }

    public SerialDiffBuilder format(String format) {
        this.format = format;
        return this;
    }

    public SerialDiffBuilder gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
        return this;
    }

    public SerialDiffBuilder lag(Integer lag) {
        this.lag = lag;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(SerialDiffParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(SerialDiffParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        if (lag != null) {
            builder.field(SerialDiffParser.LAG.getPreferredName(), lag);
        }
        return builder;
    }

}
