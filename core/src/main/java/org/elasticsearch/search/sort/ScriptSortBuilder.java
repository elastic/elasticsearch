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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Script sort builder allows to sort based on a custom script expression.
 */
public class ScriptSortBuilder extends SortBuilder {

    private Script script;

    @Deprecated
    private String scriptString;

    private final String type;

    @Deprecated
    private String lang;

    @Deprecated
    private Map<String, Object> params;

    private SortOrder order;

    private String sortMode;

    private QueryBuilder nestedFilter;

    private String nestedPath;

    /**
     * Constructs a script sort builder with the given script.
     *
     * @param script
     *            The script to use.
     */
    public ScriptSortBuilder(Script script, String type) {
        this.script = script;
        this.type = type;
    }

    /**
     * Constructs a script sort builder with the script and the type.
     *
     * @param script
     *            The script to use.
     * @param type
     *            The type, can either be "string" or "number".
     *
     * @deprecated Use {@link #ScriptSortBuilder(Script, String)} instead.
     */
    @Deprecated
    public ScriptSortBuilder(String script, String type) {
        this.scriptString = script;
        this.type = type;
    }

    /**
     * Adds a parameter to the script.
     *
     * @param name
     *            The name of the parameter.
     * @param value
     *            The value of the parameter.
     *
     * @deprecated Use {@link #ScriptSortBuilder(Script, String)} instead.
     */
    @Deprecated
    public ScriptSortBuilder param(String name, Object value) {
        if (params == null) {
            params = new HashMap<>();
        }
        params.put(name, value);
        return this;
    }

    /**
     * Sets parameters for the script.
     *
     * @param params
     *            The script parameters
     *
     * @deprecated Use {@link #ScriptSortBuilder(Script, String)} instead.
     */
    @Deprecated
    public ScriptSortBuilder setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    /**
     * The language of the script.
     *
     * @deprecated Use {@link #ScriptSortBuilder(Script, String)} instead.
     */
    @Deprecated
    public ScriptSortBuilder lang(String lang) {
        this.lang = lang;
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
    public ScriptSortBuilder setNestedFilter(QueryBuilder nestedFilter) {
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
    public XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject("_script");
        if (script == null) {

            builder.field("script", new Script(scriptString, ScriptType.INLINE, lang, params));
        } else {
            builder.field("script", script);
        }
        builder.field("type", type);
        if (order == SortOrder.DESC) {
            builder.field("reverse", true);
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
