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

public class SearchContextReducers {

        private final ReducerFactories factories;
        private Reducer[] reducers;

        /**
         * Creates a new aggregation context with the parsed aggregator factories
         */
        public SearchContextReducers(ReducerFactories factories) {
            this.factories = factories;
        }

        public ReducerFactories factories() {
            return factories;
        }

        public Reducer[] reducers() {
            return reducers;
        }

        /**
         * Registers all the created reducers (top level reducers) for the search execution context.
         *
         * @param aggregators The top level reducers of the search execution.
         */
        public void reducers(Reducer[] reducers) {
            this.reducers = reducers;
        }

}
