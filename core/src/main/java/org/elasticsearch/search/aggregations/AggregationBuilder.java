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


import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

/**
 * A factory that knows how to create an {@link Aggregator} of a specific type.
 */
public abstract class AggregationBuilder
    extends ToXContentToBytes
    implements NamedWriteable, ToXContent {

    protected final String name;
    protected final Type type;
    protected AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder();

    /**
     * Constructs a new aggregation builder.
     *
     * @param name  The aggregation name
     * @param type  The aggregation type
     */
    protected AggregationBuilder(String name, Type type) {
        if (name == null) {
            throw new IllegalArgumentException("[name] must not be null: [" + name + "]");
        }
        if (type == null) {
            throw new IllegalArgumentException("[type] must not be null: [" + name + "]");
        }
        this.name = name;
        this.type = type;
    }

    /** Return this aggregation's name. */
    public String getName() {
        return name;
    }

    /** Internal: build an {@link AggregatorFactory} based on the configuration of this builder. */
    protected abstract AggregatorFactory<?> build(AggregationContext context, AggregatorFactory<?> parent) throws IOException;

    /** Associate metadata with this {@link AggregationBuilder}. */
    public abstract AggregationBuilder setMetaData(Map<String, Object> metaData);

    /** Add a sub aggregation to this builder. */
    public abstract AggregationBuilder subAggregation(AggregationBuilder aggregation);

    /** Add a sub aggregation to this builder. */
    public abstract AggregationBuilder subAggregation(PipelineAggregationBuilder aggregation);

    /**
     * Internal: Registers sub-factories with this factory. The sub-factory will be
     * responsible for the creation of sub-aggregators under the aggregator
     * created by this factory. This is only for use by {@link AggregatorParsers}.
     *
     * @param subFactories
     *            The sub-factories
     * @return this factory (fluent interface)
     */
    protected abstract AggregationBuilder subAggregations(AggregatorFactories.Builder subFactories);

    /** Common xcontent fields shared among aggregator builders */
    public static final class CommonFields extends ParseField.CommonFields {
        public static final ParseField VALUE_TYPE = new ParseField("value_type");
    }
}
