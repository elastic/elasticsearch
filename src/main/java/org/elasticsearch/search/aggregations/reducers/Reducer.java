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

package org.elasticsearch.search.aggregations.reducers;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public abstract class Reducer {

    /**
     * Parses the reducer request and creates the appropriate reducer factory
     * for it.
     * 
     * @see {@link ReducerFactory}
     */
    public static interface Parser {

        /**
         * @return The reducer type this parser is associated with.
         */
        String type();

        /**
         * Returns the reducer factory with which this parser is associated.
         * 
         * @param reducerName
         *            The name of the reducer
         * @param parser
         *            The xcontent parser
         * @param context
         *            The search context
         * @return The resolved reducer factory
         * @throws java.io.IOException
         *             When parsing fails
         */
        ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException;

    }

    public abstract InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext);

}
