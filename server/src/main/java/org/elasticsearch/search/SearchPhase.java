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

package org.elasticsearch.search;

import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;

/**
 * Represents a phase of a search request e.g. query, fetch etc.
 */
public interface SearchPhase {

    /**
     * Performs pre processing of the search context before the execute.
     */
    void preProcess(SearchContext context);

    /**
     * Executes the search phase
     */
    void execute(SearchContext context);

    class SearchContextSourcePrinter {
        private final SearchContext searchContext;

        public SearchContextSourcePrinter(SearchContext searchContext) {
            this.searchContext = searchContext;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(searchContext.indexShard().shardId());
            builder.append(" ");
            if (searchContext.request() != null &&
                    searchContext.request().source() != null) {
                builder.append("source[").append(searchContext.request().source().toString()).append("], ");
            } else {
                builder.append("source[], ");
            }
            if (searchContext.getTask() != null &&
                    searchContext.getTask().getHeader(Task.X_OPAQUE_ID) != null) {
                builder.append("id[").append(searchContext.getTask().getHeader(Task.X_OPAQUE_ID)).append("], ");
            } else {
                builder.append("id[], ");
            }
            return builder.toString();
        }
    }
}
