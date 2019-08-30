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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.Collection;
import java.util.Map;

/**
 * A factory that knows how to create an {@link PipelineAggregator} of a
 * specific type.
 */
public abstract class PipelineAggregationBuilder implements NamedWriteable, BaseAggregationBuilder, ToXContentFragment {

    protected final String name;
    protected final String[] bucketsPaths;

    /**
     * Constructs a new pipeline aggregator factory.
     *
     * @param name
     *            The aggregation name
     */
    protected PipelineAggregationBuilder(String name, String[] bucketsPaths) {
        if (name == null) {
            throw new IllegalArgumentException("[name] must not be null: [" + name + "]");
        }
        if (bucketsPaths == null) {
            throw new IllegalArgumentException("[bucketsPaths] must not be null: [" + name + "]");
        }
        this.name = name;
        this.bucketsPaths = bucketsPaths;
    }

    /** Return this aggregation's name. */
    public String getName() {
        return name;
    }

    /** Return the consumed buckets paths. */
    public final String[] getBucketsPaths() {
        return bucketsPaths;
    }

    /**
     * Internal: Validates the state of this factory (makes sure the factory is properly
     * configured)
     */
    protected abstract void validate(AggregatorFactory parent, Collection<AggregationBuilder> aggregationBuilders,
            Collection<PipelineAggregationBuilder> pipelineAggregatorBuilders);

    /**
     * Creates the pipeline aggregator
     *
     * @return The created aggregator
     */
    protected abstract PipelineAggregator create();

    /** Associate metadata with this {@link PipelineAggregationBuilder}. */
    @Override
    public abstract PipelineAggregationBuilder setMetaData(Map<String, Object> metaData);

    @Override
    public PipelineAggregationBuilder subAggregations(Builder subFactories) {
        throw new IllegalArgumentException("Aggregation [" + name + "] cannot define sub-aggregations");
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
