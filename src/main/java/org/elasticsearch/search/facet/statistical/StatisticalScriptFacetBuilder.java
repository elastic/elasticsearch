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

package org.elasticsearch.search.facet.statistical;

import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class StatisticalScriptFacetBuilder extends FacetBuilder {
    private String lang;
    private String script;
    private Map<String, Object> params;

    public StatisticalScriptFacetBuilder(String name) {
        super(name);
    }

    /**
     * Marks the facet to run in a global scope, not bounded by any query.
     */
    public StatisticalScriptFacetBuilder global(boolean global) {
        super.global(global);
        return this;
    }

    public StatisticalScriptFacetBuilder facetFilter(FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    public StatisticalScriptFacetBuilder nested(String nested) {
        this.nested = nested;
        return this;
    }

    /**
     * The language of the script.
     */
    public StatisticalScriptFacetBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    public StatisticalScriptFacetBuilder script(String script) {
        this.script = script;
        return this;
    }

    public StatisticalScriptFacetBuilder param(String name, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(name, value);
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (script == null) {
            throw new SearchSourceBuilderException("script must be set on statistical script facet [" + name + "]");
        }
        builder.startObject(name);

        builder.startObject(StatisticalFacet.TYPE);
        builder.field("script", script);
        if (lang != null) {
            builder.field("lang", lang);
        }
        if (this.params != null) {
            builder.field("params", this.params);
        }
        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;
    }
}
