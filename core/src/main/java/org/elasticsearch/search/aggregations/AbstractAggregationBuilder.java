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
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Base implementation of a {@link AggregationBuilder}.
 */
public abstract class AbstractAggregationBuilder<AB extends AbstractAggregationBuilder<AB>>
    extends AggregationBuilder {

    protected Map<String, Object> metaData;

    /**
     * Constructs a new aggregation builder.
     *
     * @param name  The aggregation name
     * @param type  The aggregation type
     */
    public AbstractAggregationBuilder(String name, Type type) {
        super(name, type);
    }

    /**
     * Read from a stream.
     */
    protected AbstractAggregationBuilder(StreamInput in, Type type) throws IOException {
        super(in.readString(), type);
        factoriesBuilder = new AggregatorFactories.Builder(in);
        metaData = in.readMap();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        factoriesBuilder.writeTo(out);
        out.writeMap(metaData);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @SuppressWarnings("unchecked")
    @Override
    public AB subAggregation(AggregationBuilder aggregation) {
        if (aggregation == null) {
            throw new IllegalArgumentException("[aggregation] must not be null: [" + name + "]");
        }
        factoriesBuilder.addAggregator(aggregation);
        return (AB) this;
    }

    /**
     * Add a sub aggregation to this aggregation.
     */
    @SuppressWarnings("unchecked")
    @Override
    public AB subAggregation(PipelineAggregationBuilder aggregation) {
        if (aggregation == null) {
            throw new IllegalArgumentException("[aggregation] must not be null: [" + name + "]");
        }
        factoriesBuilder.addPipelineAggregator(aggregation);
        return (AB) this;
    }

    /**
     * Registers sub-factories with this factory. The sub-factory will be
     * responsible for the creation of sub-aggregators under the aggregator
     * created by this factory.
     *
     * @param subFactories
     *            The sub-factories
     * @return this factory (fluent interface)
     */
    @SuppressWarnings("unchecked")
    @Override
    public AB subAggregations(AggregatorFactories.Builder subFactories) {
        if (subFactories == null) {
            throw new IllegalArgumentException("[subFactories] must not be null: [" + name + "]");
        }
        this.factoriesBuilder = subFactories;
        return (AB) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public AB setMetaData(Map<String, Object> metaData) {
        if (metaData == null) {
            throw new IllegalArgumentException("[metaData] must not be null: [" + name + "]");
        }
        this.metaData = metaData;
        return (AB) this;
    }

    public String getType() {
        return type.name();
    }

    @Override
    public final AggregatorFactory<?> build(AggregationContext context, AggregatorFactory<?> parent) throws IOException {
        AggregatorFactory<?> factory = doBuild(context, parent, factoriesBuilder);
        return factory;
    }

    protected abstract AggregatorFactory<?> doBuild(AggregationContext context, AggregatorFactory<?> parent,
            AggregatorFactories.Builder subfactoriesBuilder) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);

        if (this.metaData != null) {
            builder.field("meta", this.metaData);
        }
        builder.field(type.name());
        internalXContent(builder, params);

        if (factoriesBuilder != null && (factoriesBuilder.count()) > 0) {
            builder.field("aggregations");
            factoriesBuilder.toXContent(builder, params);

        }

        return builder.endObject();
    }

    protected abstract XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(factoriesBuilder, metaData, name, type, doHashCode());
    }

    protected abstract int doHashCode();

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("unchecked")
        AbstractAggregationBuilder<AB> other = (AbstractAggregationBuilder<AB>) obj;
        if (!Objects.equals(name, other.name))
            return false;
        if (!Objects.equals(type, other.type))
            return false;
        if (!Objects.equals(metaData, other.metaData))
            return false;
        if (!Objects.equals(factoriesBuilder, other.factoriesBuilder))
            return false;
        return doEquals(obj);
    }

    protected abstract boolean doEquals(Object obj);

}
