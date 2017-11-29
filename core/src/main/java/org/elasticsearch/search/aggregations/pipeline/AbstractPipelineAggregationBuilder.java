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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
    public final void validate(AggregatorFactory<?> parent, List<AggregationBuilder> factories,
            List<PipelineAggregationBuilder> pipelineAggregatorFactories) {
        doValidate(parent, factories, pipelineAggregatorFactories);
    }

    protected abstract PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException;

    /**
     * Creates the pipeline aggregator
     *
     * @return The created aggregator
     */
    @Override
    public final PipelineAggregator create() throws IOException {
        PipelineAggregator aggregator = createInternal(this.metaData);
        return aggregator;
    }

    public void doValidate(AggregatorFactory<?> parent, List<AggregationBuilder> factories,
            List<PipelineAggregationBuilder> pipelineAggregatorFactories) {
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
        return Objects.hash(Arrays.hashCode(bucketsPaths), metaData, name, type, doHashCode());
    }

    protected abstract int doHashCode();

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("unchecked")
        AbstractPipelineAggregationBuilder<PAB> other = (AbstractPipelineAggregationBuilder<PAB>) obj;
        if (!Objects.equals(name, other.name))
            return false;
        if (!Objects.equals(type, other.type))
            return false;
        if (!Objects.deepEquals(bucketsPaths, other.bucketsPaths))
            return false;
        if (!Objects.equals(metaData, other.metaData))
            return false;
        return doEquals(obj);
    }

    protected abstract boolean doEquals(Object obj);

    @Override
    public String getType() {
        return type;
    }
}
