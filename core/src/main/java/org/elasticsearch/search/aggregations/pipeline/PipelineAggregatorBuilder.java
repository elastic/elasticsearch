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

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A factory that knows how to create an {@link PipelineAggregator} of a
 * specific type.
 */
public abstract class PipelineAggregatorBuilder<PAB extends PipelineAggregatorBuilder<PAB>> extends ToXContentToBytes
        implements NamedWriteable<PipelineAggregatorBuilder<PAB>>, ToXContent {

    protected final String name;
    protected final String type;
    protected final String[] bucketsPaths;
    protected Map<String, Object> metaData;

    /**
     * Constructs a new pipeline aggregator factory.
     *
     * @param name
     *            The aggregation name
     * @param type
     *            The aggregation type
     */
    public PipelineAggregatorBuilder(String name, String type, String[] bucketsPaths) {
        if (name == null) {
            throw new IllegalArgumentException("[name] must not be null: [" + name + "]");
        }
        if (type == null) {
            throw new IllegalArgumentException("[type] must not be null: [" + name + "]");
        }
        if (bucketsPaths == null) {
            throw new IllegalArgumentException("[bucketsPaths] must not be null: [" + name + "]");
        }
        this.name = name;
        this.type = type;
        this.bucketsPaths = bucketsPaths;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    /**
     * Validates the state of this factory (makes sure the factory is properly
     * configured)
     */
    public final void validate(AggregatorFactory<?> parent, AggregatorFactory<?>[] factories,
            List<PipelineAggregatorBuilder<?>> pipelineAggregatorFactories) {
        doValidate(parent, factories, pipelineAggregatorFactories);
    }

    protected abstract PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException;

    /**
     * Creates the pipeline aggregator
     *
     * @return The created aggregator
     */
    public final PipelineAggregator create() throws IOException {
        PipelineAggregator aggregator = createInternal(this.metaData);
        return aggregator;
    }

    public void doValidate(AggregatorFactory<?> parent, AggregatorFactory<?>[] factories,
            List<PipelineAggregatorBuilder<?>> pipelineAggregatorFactories) {
    }

    @SuppressWarnings("unchecked")
    public PAB setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
        return (PAB) this;
    }

    public String getName() {
        return name;
    }

    public String[] getBucketsPaths() {
        return bucketsPaths;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(bucketsPaths);
        doWriteTo(out);
        out.writeMap(metaData);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public String getWriteableName() {
        return type;
    }

    @Override
    public PipelineAggregatorBuilder<PAB> readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        String[] bucketsPaths = in.readStringArray();
        PipelineAggregatorBuilder<PAB> factory = doReadFrom(name, bucketsPaths, in);
        factory.metaData = in.readMap();
        return factory;
    }

    protected abstract PipelineAggregatorBuilder<PAB> doReadFrom(String name, String[] bucketsPaths, StreamInput in) throws IOException;

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
     * @return <code>true</code> if the {@link PipelineAggregatorBuilder}
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
        PipelineAggregatorBuilder<PAB> other = (PipelineAggregatorBuilder<PAB>) obj;
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

}
