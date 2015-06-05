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

package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ScriptQueryBuilder extends QueryBuilder {

    private Script script;

    @Deprecated
    private String scriptString;

    @Deprecated
    private Map<String, Object> params;

    @Deprecated
    private String lang;

    private String queryName;

    public ScriptQueryBuilder(Script script) {
        this.script = script;
    }

    /**
     * @deprecated Use {@link #ScriptQueryBuilder(Script)} instead.
     */
    @Deprecated
    public ScriptQueryBuilder(String script) {
        this.scriptString = script;
    }

    /**
     * @deprecated Use {@link #ScriptQueryBuilder(Script)} instead.
     */
    @Deprecated
    public ScriptQueryBuilder addParam(String name, Object value) {
        if (params == null) {
            params = new HashMap<>();
        }
        params.put(name, value);
        return this;
    }

    /**
     * @deprecated Use {@link #ScriptQueryBuilder(Script)} instead.
     */
    @Deprecated
    public ScriptQueryBuilder params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = params;
        } else {
            this.params.putAll(params);
        }
        return this;
    }

    /**
     * Sets the script language.
     * 
     * @deprecated Use {@link #ScriptQueryBuilder(Script)} instead.
     */
    @Deprecated
    public ScriptQueryBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public ScriptQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params builderParams) throws IOException {

        builder.startObject(ScriptQueryParser.NAME);
        if (script != null) {
            builder.field(ScriptField.SCRIPT.getPreferredName(), script);
        } else {
            if (this.scriptString != null) {
                builder.field("script", scriptString);
            }
            if (this.params != null) {
                builder.field("params", this.params);
            }
            if (this.lang != null) {
                builder.field("lang", lang);
            }
        }

        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }
}