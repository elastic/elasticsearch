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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A function that uses a script to compute or influence the score of documents
 * that match with the inner query or filter.
 */
public class ScriptScoreFunctionBuilder extends ScoreFunctionBuilder {

    private Script script;

    private String scriptString;

    private String lang;

    private Map<String, Object> params = null;

    public ScriptScoreFunctionBuilder() {

    }

    public ScriptScoreFunctionBuilder script(Script script) {
        this.script = script;
        return this;
    }

    /**
     * @deprecated Use {@link #script(Script)} instead
     */
    @Deprecated
    public ScriptScoreFunctionBuilder script(String script) {
        this.scriptString = script;
        return this;
    }

    /**
     * Sets the language of the script.@deprecated Use {@link #script(Script)}
     * instead
     */
    @Deprecated
    public ScriptScoreFunctionBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    /**
     * Additional parameters that can be provided to the script.@deprecated Use
     * {@link #script(Script)} instead
     */
    @Deprecated
    public ScriptScoreFunctionBuilder params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = params;
        } else {
            this.params.putAll(params);
        }
        return this;
    }

    /**
     * Additional parameters that can be provided to the script.@deprecated Use
     * {@link #script(Script)} instead
     */
    @Deprecated
    public ScriptScoreFunctionBuilder param(String key, Object value) {
        if (params == null) {
            params = new HashMap<>();
        }
        params.put(key, value);
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        if (script != null) {
            builder.field(ScriptField.SCRIPT.getPreferredName(), script);
        } else {
            if (scriptString != null) {
                builder.field("script", scriptString);
            }
            if (lang != null) {
                builder.field("lang", lang);
            }
            if (this.params != null) {
                builder.field("params", this.params);
            }
        }
        builder.endObject();
    }

    @Override
    public String getName() {
        return ScriptScoreFunctionParser.NAMES[0];
    }
}