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

import org.elasticsearch.search.aggregations.support.AggregationContext;

/**
 * A factory that knows how to create an {@link Aggregator} of a specific type.
 */
public abstract class AggregatorFactory {

    protected String name;
    protected String type;
    protected AggregatorFactory parent;
    protected AggregatorFactories factories = AggregatorFactories.EMPTY;

    /**
     * Constructs a new aggregator factory.
     *
     * @param name  The aggregation name
     * @param type  The aggregation type
     */
    public AggregatorFactory(String name, String type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Registers sub-factories with this factory. The sub-factory will be responsible for the creation of sub-aggregators under the
     * aggregator created by this factory.
     *
     * @param subFactories  The sub-factories
     * @return  this factory (fluent interface)
     */
    public AggregatorFactory subFactories(AggregatorFactories subFactories) {
        this.factories = subFactories;
        this.factories.setParent(this);
        return this;
    }

    /**
     * Validates the state of this factory (makes sure the factory is properly configured)
     */
    public final void validate() {
        doValidate();
        factories.validate();
    }

    /**
     * @return  The parent factory if one exists (will always return {@code null} for top level aggregator factories).
     */
    public AggregatorFactory parent() {
        return parent;
    }

    /**
     * Creates the aggregator
     *
     * @param context               The aggregation context
     * @param parent                The parent aggregator (if this is a top level factory, the parent will be {@code null})
     * @param expectedBucketsCount  If this is a sub-factory of another factory, this will indicate the number of bucket the parent aggregator
     *                              may generate (this is an estimation only). For top level factories, this will always be 0
     *
     * @return                      The created aggregator
     */
    public abstract Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount);

    public void doValidate() {
    }

}
