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

package org.elasticsearch.search.facet;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;

import java.io.IOException;

/**
 *
 */
public abstract class FacetBuilder implements ToXContent {

    public static enum Mode {
        COLLECTOR() {
            @Override
            public String toString() {
                return "collector";
            }
        },
        POST() {
            @Override
            public String toString() {
                return "post";
            }
        };

        public abstract String toString();
    }

    protected final String name;
    protected FilterBuilder facetFilter;
    protected Boolean global;
    protected String nested;
    protected Mode mode;

    protected FacetBuilder(String name) {
        this.name = name;
    }

    public FacetBuilder facetFilter(FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    public FacetBuilder nested(String nested) {
        this.nested = nested;
        return this;
    }

    /**
     * Marks the facet to run in a global scope, not bounded by any query.
     */
    public FacetBuilder global(boolean global) {
        this.global = global;
        return this;
    }

    public FacetBuilder mode(Mode mode) {
        this.mode = mode;
        return this;
    }

    protected void addFilterFacetAndGlobal(XContentBuilder builder, Params params) throws IOException {
        if (facetFilter != null) {
            builder.field("facet_filter");
            facetFilter.toXContent(builder, params);
        }
        if (nested != null) {
            builder.field("nested", nested);
        }
        if (global != null) {
            builder.field("global", global);
        }
        if (mode != null) {
            builder.field("mode", mode.toString());
        }
    }
}
