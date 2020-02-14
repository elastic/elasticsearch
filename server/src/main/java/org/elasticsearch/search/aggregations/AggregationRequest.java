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
import org.elasticsearch.common.xcontent.ToXContentFragment;

import java.util.Collection;
import java.util.Map;

public abstract class AggregationRequest implements ToXContentFragment, BaseAggregationBuilder {
    // NOCOMMIT can these be private?
    protected final String name;
    protected AggregatorFactories.Builder factoriesBuilder;

    /**
     * Constructs a new aggregation request.
     *
     * @param name  The aggregation name
     */
    protected AggregationRequest(String name) {
        this(name, AggregatorFactories.builder());
    }

    /**
     * Constructs a new aggregation request.
     *
     * @param name  The aggregation name
     * @param factoriesBuilder sub-aggregations
     */
    protected AggregationRequest(String name, AggregatorFactories.Builder factoriesBuilder) {
        if (name == null) {
            throw new IllegalArgumentException("[name] must not be null: [" + name + "]");
        }
        this.name = name;
        this.factoriesBuilder = factoriesBuilder;
    }

    /** Return this aggregation's name. */
    public String getName() {
        return name;
    }

    /** Associate metadata with this {@link AggregationBuilder}. */
    @Override
    public abstract AggregationBuilder setMetaData(Map<String, Object> metaData);

    /** Return any associated metadata with this {@link AggregationBuilder}. */
    public abstract Map<String, Object> getMetaData();

    /** Add a sub aggregation to this builder. */
    public abstract AggregationBuilder subAggregation(AggregationBuilder aggregation);

    /** Add a sub aggregation to this builder. */
    public abstract AggregationBuilder subAggregation(PipelineAggregationBuilder aggregation);

    /** Return the configured set of subaggregations **/
    public Collection<AggregationBuilder> getSubAggregations() {
        return factoriesBuilder.getAggregatorFactories();
    }

    /** Return the configured set of pipeline aggregations **/
    public Collection<PipelineAggregationBuilder> getPipelineAggregations() {
        return factoriesBuilder.getPipelineAggregatorFactories();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
