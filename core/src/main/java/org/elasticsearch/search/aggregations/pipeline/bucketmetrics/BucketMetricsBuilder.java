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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketParser;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativeParser;

import java.io.IOException;

/**
 * A builder for building requests for a {@link BucketMetricsPipelineAggregator}
 */
public abstract class BucketMetricsBuilder<B extends BucketMetricsBuilder<B>> extends PipelineAggregatorBuilder<B> {

    private String format;
    private GapPolicy gapPolicy;

    public BucketMetricsBuilder(String name, String type) {
        super(name, type);
    }

    public B format(String format) {
        this.format = format;
        return (B) this;
    }

    public B gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
        return (B) this;
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(MinBucketParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(DerivativeParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        doInternalXContent(builder, params);
        return builder;
    }

    protected void doInternalXContent(XContentBuilder builder, Params params) throws IOException {
    }

}