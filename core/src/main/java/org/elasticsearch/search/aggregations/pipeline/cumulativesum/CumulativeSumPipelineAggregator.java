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
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.AbstractHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsParser;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class CumulativeSumPipelineAggregator extends PipelineAggregator {

    public final static Type TYPE = new Type("cumulative_sum");

    public final static PipelineAggregatorStreams.Stream STREAM = in -> {
        CumulativeSumPipelineAggregator result = new CumulativeSumPipelineAggregator();
        result.readFrom(in);
        return result;
    };

    public static void registerStreams() {
        PipelineAggregatorStreams.registerStream(STREAM, TYPE.stream());
    }

    private ValueFormatter formatter;

    public CumulativeSumPipelineAggregator() {
    }

    public CumulativeSumPipelineAggregator(String name, String[] bucketsPaths, ValueFormatter formatter,
            Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalHistogram histo = (InternalHistogram) aggregation;
        List<? extends InternalHistogram.Bucket> buckets = histo.getBuckets();
        InternalHistogram.Factory<? extends InternalHistogram.Bucket> factory = histo.getFactory();

        List newBuckets = new ArrayList<>();
        double sum = 0;
        for (InternalHistogram.Bucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], GapPolicy.INSERT_ZEROS);
            sum += thisBucketValue;
            List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map((p) -> {
                return (InternalAggregation) p;
            }).collect(Collectors.toList());
            aggs.add(new InternalSimpleValue(name(), sum, formatter, new ArrayList<PipelineAggregator>(), metaData()));
            InternalHistogram.Bucket newBucket = factory.createBucket(bucket.getKey(), bucket.getDocCount(),
                    new InternalAggregations(aggs), bucket.getKeyed(), bucket.getFormatter());
            newBuckets.add(newBucket);
        }
        return factory.create(newBuckets, histo);
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = ValueFormatterStreams.readOptional(in);
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
    }

    public static class CumulativeSumPipelineAggregatorBuilder extends PipelineAggregatorBuilder {

        private String format;

        public CumulativeSumPipelineAggregatorBuilder(String name, String bucketsPath) {
            this(name, new String[] { bucketsPath });
        }

        private CumulativeSumPipelineAggregatorBuilder(String name, String[] bucketsPaths) {
            super(name, TYPE.name(), bucketsPaths);
        }

        /**
         * Sets the format to use on the output of this aggregation.
         */
        public CumulativeSumPipelineAggregatorBuilder format(String format) {
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

        @Override
        protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
            return new CumulativeSumPipelineAggregator(name, bucketsPaths, formatter(), metaData);
        }

        @Override
        public void doValidate(AggregatorFactory<?> parent, AggregatorFactory<?>[] aggFactories, List<PipelineAggregatorBuilder> pipelineAggregatorFactories) {
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
        protected final PipelineAggregatorBuilder doReadFrom(String name, String[] bucketsPaths, StreamInput in) throws IOException {
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
}
