/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.facet;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.Nullable;

import java.util.List;

/**
 *
 */
public class SearchContextFacets {

    public static class Entry {
        private final String facetName;
        private final FacetExecutor.Mode mode;
        private final FacetExecutor facetExecutor;
        private final boolean global;
        @Nullable
        private final Filter filter;

        public Entry(String facetName, FacetExecutor.Mode mode, FacetExecutor facetExecutor, boolean global, @Nullable Filter filter) {
            this.facetName = facetName;
            this.mode = mode;
            this.facetExecutor = facetExecutor;
            this.global = global;
            this.filter = filter;
        }

        public String getFacetName() {
            return facetName;
        }

        public FacetExecutor.Mode getMode() {
            return mode;
        }

        public FacetExecutor getFacetExecutor() {
            return facetExecutor;
        }

        public boolean isGlobal() {
            return global;
        }

        public Filter getFilter() {
            return filter;
        }
    }

    private final List<Entry> entries;

    private boolean hasQuery;
    private boolean hasGlobal;

    public SearchContextFacets(List<Entry> entries) {
        this.entries = entries;
        for (Entry entry : entries) {
            if (entry.global) {
                hasGlobal = true;
            } else {
                hasQuery = true;
            }
        }
    }

    public List<Entry> entries() {
        return this.entries;
    }

    /**
     * Are there facets that need to be computed on the query hits?
     */
    public boolean hasQuery() {
        return hasQuery;
    }

    /**
     * Are there global facets that need to be computed on all the docs.
     */
    public boolean hasGlobal() {
        return hasGlobal;
    }
}
