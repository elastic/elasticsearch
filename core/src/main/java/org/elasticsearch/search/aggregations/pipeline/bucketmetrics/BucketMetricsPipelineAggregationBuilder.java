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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class BucketMetricsPipelineAggregationBuilder<AF extends BucketMetricsPipelineAggregationBuilder<AF>>
        extends AbstractPipelineAggregationBuilder<AF> {

    private String format = null;
    private GapPolicy gapPolicy = GapPolicy.SKIP;

    protected BucketMetricsPipelineAggregationBuilder(String name, String type, String[] bucketsPaths) {
        super(name, type, bucketsPaths);
    }

    /**
     * Read from a stream.
     */
    protected BucketMetricsPipelineAggregationBuilder(StreamInput in, String type) throws IOException {
        super(in, type);
        format = in.readOptionalString();
        gapPolicy = GapPolicy.readFrom(in);
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);
        innerWriteTo(out);
    }

    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    /**
     * Sets the format to use on the output of this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AF format(String format) {
        this.format = format;
        return (AF) this;
    }

    /**
     * Gets the format to use on the output of this aggregation.
     */
    public String format() {
        return format;
    }

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return DocValueFormat.RAW;
        }
    }

    /**
     * Sets the gap policy to use for this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AF gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
        return (AF) this;
    }

    /**
     * Gets the gap policy to use for this aggregation.
     */
    public GapPolicy gapPolicy() {
        return gapPolicy;
    }

    @Override
    protected abstract PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException;

    @Override
    public void doValidate(AggregatorFactory<?> parent, AggregatorFactory<?>[] aggFactories,
            List<PipelineAggregationBuilder> pipelineAggregatorFactories) {
        if (bucketsPaths.length != 1) {
            throw new IllegalStateException(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " must contain a single entry for aggregation [" + name + "]");
        }
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(BucketMetricsParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(BucketMetricsParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        doXContentBody(builder, params);
        return builder;
    }

    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    protected final int doHashCode() {
        return Objects.hash(format, gapPolicy, innerHashCode());
    }

    protected abstract int innerHashCode();

    @Override
    protected final boolean doEquals(Object obj) {
        @SuppressWarnings("unchecked")
        BucketMetricsPipelineAggregationBuilder<AF> other = (BucketMetricsPipelineAggregationBuilder<AF>) obj;
        return Objects.equals(format, other.format)
                && Objects.equals(gapPolicy, other.gapPolicy)
                && innerEquals(other);
    }

    protected abstract boolean innerEquals(BucketMetricsPipelineAggregationBuilder<AF> other);

}
