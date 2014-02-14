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

package org.elasticsearch.search.sort;

import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Script sort builder allows to sort based on a custom script expression.
 */
public class ScriptSortBuilder extends SortBuilder {

    private String lang;

    private final String script;

    private final String type;

    private SortOrder order;

    private Map<String, Object> params;

    private String sortMode;

    private FilterBuilder nestedFilter;

    private String nestedPath;

    /**
     * Constructs a script sort builder with the script and the type.
     *
     * @param script The script to use.
     * @param type   The type, can either be "string" or "number".
     */
    public ScriptSortBuilder(String script, String type) {
        this.script = script;
        this.type = type;
    }

    /**
     * Adds a parameter to the script.
     *
     * @param name  The name of the parameter.
     * @param value The value of the parameter.
     */
    public ScriptSortBuilder param(String name, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(name, value);
        return this;
    }

    /**
     * Sets parameters for the script.
     *
     * @param params The script parameters
     */
    public ScriptSortBuilder setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    /**
     * Sets the sort order.
     */
    @Override
    public ScriptSortBuilder order(SortOrder order) {
        this.order = order;
        return this;
    }

    /**
     * Not really relevant.
     */
    @Override
    public SortBuilder missing(Object missing) {
        return this;
    }

    /**
     * The language of the script.
     */
    public ScriptSortBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    /**
     * Defines which distance to use for sorting in the case a document contains multiple geo points.
     * Possible values: min and max
     */
    public ScriptSortBuilder sortMode(String sortMode) {
        this.sortMode = sortMode;
        return this;
    }

    /**
     * Sets the nested filter that the nested objects should match with in order to be taken into account
     * for sorting.
     */
    public ScriptSortBuilder setNestedFilter(FilterBuilder nestedFilter) {
        this.nestedFilter = nestedFilter;
        return this;
    }

    /**
     * Sets the nested path if sorting occurs on a field that is inside a nested object. For sorting by script this
     * needs to be specified.
     */
    public ScriptSortBuilder setNestedPath(String nestedPath) {
        this.nestedPath = nestedPath;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("_script");
        builder.field("script", script);
        builder.field("type", type);
        if (order == SortOrder.DESC) {
            builder.field("reverse", true);
        }
        if (lang != null) {
            builder.field("lang", lang);
        }
        if (this.params != null) {
            builder.field("params", this.params);
        }
        if (sortMode != null) {
            builder.field("mode", sortMode);
        }
        if (nestedPath != null) {
            builder.field("nested_path", nestedPath);
        }
        if (nestedFilter != null) {
            builder.field("nested_filter", nestedFilter, params);
        }
        builder.endObject();
        return builder;
    }
}
