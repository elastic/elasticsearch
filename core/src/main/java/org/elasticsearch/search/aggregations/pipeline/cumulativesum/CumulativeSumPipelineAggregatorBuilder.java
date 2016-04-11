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

package org.elasticsearch.search.aggregations.pipeline.cumulativesum;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.AbstractHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CumulativeSumPipelineAggregatorBuilder extends PipelineAggregatorBuilder<CumulativeSumPipelineAggregatorBuilder> {

    static final CumulativeSumPipelineAggregatorBuilder PROTOTYPE = new CumulativeSumPipelineAggregatorBuilder("", "");

    private String format;

    public CumulativeSumPipelineAggregatorBuilder(String name, String bucketsPath) {
        this(name, new String[] { bucketsPath });
    }

    private CumulativeSumPipelineAggregatorBuilder(String name, String[] bucketsPaths) {
        super(name, CumulativeSumPipelineAggregator.TYPE.name(), bucketsPaths);
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public CumulativeSumPipelineAggregatorBuilder format(String format) {
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

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return DocValueFormat.RAW;
        }
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
        return new CumulativeSumPipelineAggregator(name, bucketsPaths, formatter(), metaData);
    }

    @Override
    public void doValidate(AggregatorFactory<?> parent, AggregatorFactory<?>[] aggFactories,
            List<PipelineAggregatorBuilder<?>> pipelineAggregatorFactories) {
        if (bucketsPaths.length != 1) {
            throw new IllegalStateException(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " must contain a single entry for aggregation [" + name + "]");
        }
        if (!(parent instanceof AbstractHistogramAggregatorFactory<?>)) {
            throw new IllegalStateException("cumulative sum aggregation [" + name
                    + "] must have a histogram or date_histogram as parent");
        } else {
            AbstractHistogramAggregatorFactory<?> histoParent = (AbstractHistogramAggregatorFactory<?>) parent;
            if (histoParent.minDocCount() != 0) {
                throw new IllegalStateException("parent histogram of cumulative sum aggregation [" + name
                        + "] must have min_doc_count of 0");
            }
        }
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(BucketMetricsParser.FORMAT.getPreferredName(), format);
        }
        return builder;
    }

    @Override
    protected final CumulativeSumPipelineAggregatorBuilder doReadFrom(String name, String[] bucketsPaths, StreamInput in)
            throws IOException {
        CumulativeSumPipelineAggregatorBuilder factory = new CumulativeSumPipelineAggregatorBuilder(name, bucketsPaths);
        factory.format = in.readOptionalString();
        return factory;
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(format);
    }

    @Override
    protected boolean doEquals(Object obj) {
        CumulativeSumPipelineAggregatorBuilder other = (CumulativeSumPipelineAggregatorBuilder) obj;
        return Objects.equals(format, other.format);
    }
}