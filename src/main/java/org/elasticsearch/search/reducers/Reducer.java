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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public abstract class Reducer implements Releasable{

    private String name;
    private ReducerFactories factories;
    private SearchContext context;
    private Reducer parent;
    private Reducer[] subReducers;
    private HashMap<String, Reducer> subReducersbyName;

    public Reducer(String name, ReducerFactories factories, SearchContext context, Reducer parent) {
        this.name = name;
        assert factories != null : "sub-factories provided to Reducer must not be null, use ReducerFactories.EMPTY instead";
        this.factories = factories;
        this.context = context;
        this.parent = parent;
        this.subReducers = factories.createSubReducers(parent);
        context.addReleasable(this, Lifetime.PHASE);
    }

    public String name() {
        return name;
    }

    public Reducer parent() {
        return parent;
    }

    public Reducer[] subReducers() {
        return subReducers;
    }

    public Reducer subReducer(String reducerName) {
        if (subReducersbyName == null) {
            subReducersbyName = new HashMap<>(subReducers.length);
            for (int i = 0; i < subReducers.length; i++) {
                subReducersbyName.put(subReducers[i].name, subReducers[i]);
            }
        }
        return subReducersbyName.get(reducerName);
    }

    public SearchContext context() {
        return context;
    }

    public void preReduce() throws ReductionInitializationException {
        // Default Implementation does nothing
    }

    public abstract InternalAggregation reduce(List<Aggregation> aggregations, ReduceContext reduceContext) throws ReductionExecutionException;

    @Override
    public void close() throws ElasticsearchException {
    }

    /**
     * Parses the reducer request and creates the appropriate reducer factory for it.
     *
     * @see {@link ReducerFactory}
    */
    public static interface Parser {

        /**
         * @return The reducer type this parser is associated with.
         */
        String type();

        /**
         * Returns the reducer factory with which this parser is associated, may return {@code null} indicating the
         * reducer should be skipped (e.g. when trying to aggregate on unmapped fields).
         *
         * @param reducerName   The name of the reducer
         * @param parser            The xcontent parser
         * @param context           The search context
         * @return                  The resolved reducer factory or {@code null} in case the reducer should be skipped
         * @throws java.io.IOException      When parsing fails
         */
        ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException;

    }

}
