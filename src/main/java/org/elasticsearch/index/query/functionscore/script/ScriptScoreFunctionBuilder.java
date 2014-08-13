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

package org.elasticsearch.index.query.functionscore.script;

import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;

import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * A function that uses a script to compute or influence the score of documents
 * that match with the inner query or filter.
 */
public class ScriptScoreFunctionBuilder extends ScoreFunctionBuilder {

    private String script;

    private String lang;

    private Map<String, Object> params = null;

    public ScriptScoreFunctionBuilder() {

    }

    public ScriptScoreFunctionBuilder script(String script) {
        this.script = script;
        return this;
    }

    /**
     * Sets the language of the script.
     */
    public ScriptScoreFunctionBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    /**
     * Additional parameters that can be provided to the script.
     */
    public ScriptScoreFunctionBuilder params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = params;
        } else {
            this.params.putAll(params);
        }
        return this;
    }

    /**
     * Additional parameters that can be provided to the script.
     */
    public ScriptScoreFunctionBuilder param(String key, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(key, value);
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field("script", script);
        if (lang != null) {
            builder.field("lang", lang);
        }
        if (this.params != null) {
            builder.field("params", this.params);
        }
        builder.endObject();
    }

    @Override
    public String getName() {
        return ScriptScoreFunctionParser.NAMES[0];
    }
}