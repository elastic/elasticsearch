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
package org.elasticsearch.search.reducers;

import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;


/**
 * A factory that knows how to create an {@link Reducer} of a specific type.
 */
public abstract class ReducerFactory implements Streamable {

    protected String name;
    protected Type type;
    protected ReducerFactories factories = ReducerFactories.EMPTY;

    protected ReducerFactory(Type type) {
        this.type = type;
    }

    /**
     * Constructs a new reducer factory.
     *
     * @param name  The reducer name
     * @param type  The reducer type
     */
    public ReducerFactory(String name, Type type) {
        this(type);
        this.name = name;
    }

    public Type type() {
        return type;
    }

    /**
     * Registers sub-factories with this factory. The sub-factory will be responsible for the creation of sub-reducers under the
     * reducer created by this factory.
     *
     * @param subFactories  The sub-factories
     * @return  this factory (fluent interface)
     */
    public ReducerFactory subFactories(ReducerFactories subFactories) {
        this.factories = subFactories;
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
     * Creates the reducer
     *
     * @param context               The reducer context
     * @param parent                The parent reducer (if this is a top level factory, the parent will be {@code null})
     * @param expectedBucketsCount  If this is a sub-factory of another factory, this will indicate the number of bucket the parent reducer
     *                              may generate (this is an estimation only). For top level factories, this will always be 0
     *
     * @return                      The created reducer
     */
    public abstract Reducer create(ReducerContext context, Reducer parent);

    public void doValidate() {
    }

}
