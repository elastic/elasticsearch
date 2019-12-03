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
package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregatorFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Base implementation of a {@link PipelineAggregationBuilder}.
 */
public abstract class AbstractPipelineAggregationBuilder<PAB extends AbstractPipelineAggregationBuilder<PAB>>
        extends PipelineAggregationBuilder {

    /**
     * Field shared by many parsers.
     */
    public static final ParseField BUCKETS_PATH_FIELD = new ParseField("buckets_path");

    protected final String type;
    protected Map<String, Object> metaData;

    protected AbstractPipelineAggregationBuilder(String name, String type, String[] bucketsPaths) {
        super(name, bucketsPaths);
        if (type == null) {
            throw new IllegalArgumentException("[type] must not be null: [" + name + "]");
        }
        this.type = type;
    }

    /**
     * Read from a stream.
     */
    protected AbstractPipelineAggregationBuilder(StreamInput in, String type) throws IOException {
        this(in.readString(), type, in.readStringArray());
        metaData = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(bucketsPaths);
        out.writeMap(metaData);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    public String type() {
        return type;
    }

    /**
     * Validates the state of this factory (makes sure the factory is properly
     * configured)
     */
    @Override
    public final void validate(AggregatorFactory parent, Collection<AggregationBuilder> factories,
            Collection<PipelineAggregationBuilder> pipelineAggregatorFactories) {
        doValidate(parent, factories, pipelineAggregatorFactories);
    }

    protected abstract PipelineAggregator createInternal(Map<String, Object> metaData);

    /**
     * Creates the pipeline aggregator
     *
     * @return The created aggregator
     */
    @Override
    public final PipelineAggregator create() {
        PipelineAggregator aggregator = createInternal(this.metaData);
        return aggregator;
    }

    public void doValidate(AggregatorFactory parent, Collection<AggregationBuilder> factories,
            Collection<PipelineAggregationBuilder> pipelineAggregatorFactories) {
    }

    /**
     * Validates pipeline aggregations that need sequentially ordered data.
     */
    public static void validateSequentiallyOrderedParentAggs(AggregatorFactory parent, String type, String name) {
        if ((parent instanceof HistogramAggregatorFactory || parent instanceof DateHistogramAggregatorFactory
                || parent instanceof AutoDateHistogramAggregatorFactory) == false) {
            throw new IllegalStateException(
                    type + " aggregation [" + name + "] must have a histogram, date_histogram or auto_date_histogram as parent");
        }
        if (parent instanceof HistogramAggregatorFactory) {
            HistogramAggregatorFactory histoParent = (HistogramAggregatorFactory) parent;
            if (histoParent.minDocCount() != 0) {
                throw new IllegalStateException("parent histogram of " + type + " aggregation [" + name + "] must have min_doc_count of 0");
            }
        } else if (parent instanceof DateHistogramAggregatorFactory) {
            DateHistogramAggregatorFactory histoParent = (DateHistogramAggregatorFactory) parent;
            if (histoParent.minDocCount() != 0) {
                throw new IllegalStateException("parent histogram of " + type + " aggregation [" + name + "] must have min_doc_count of 0");
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public PAB setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
        return (PAB) this;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());

        if (this.metaData != null) {
            builder.field("meta", this.metaData);
        }
        builder.startObject(type);

        if (!overrideBucketsPath() && bucketsPaths != null) {
            builder.startArray(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName());
            for (String path : bucketsPaths) {
                builder.value(path);
            }
            builder.endArray();
        }

        internalXContent(builder, params);

        builder.endObject();

        return builder.endObject();
    }

    /**
     * @return <code>true</code> if the {@link AbstractPipelineAggregationBuilder}
     *         overrides the XContent rendering of the bucketPath option.
     */
    protected boolean overrideBucketsPath() {
        return false;
    }

    protected abstract XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(bucketsPaths), metaData, name, type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AbstractPipelineAggregationBuilder<PAB> other = (AbstractPipelineAggregationBuilder<PAB>) obj;
        return Objects.equals(type, other.type)
            && Objects.equals(name, other.name)
            && Objects.equals(metaData, other.metaData)
            && Objects.deepEquals(bucketsPaths, other.bucketsPaths);
    }

    @Override
    public String getType() {
        return type;
    }
}
