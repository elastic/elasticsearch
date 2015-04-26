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

package org.elasticsearch.script;

import java.util.Map;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

import static org.elasticsearch.script.ScriptService.ScriptType;

/**
 * Script holds all the parameters necessary to compile or find in cache and then execute a script.
 */
public class Script {

    private final String lang;
    private final String script;
    private final ScriptType type;
    private final Map<String, Object> params;

    /**
     * Constructor for Script.
     * @param lang The language of the script to be compiled/executed.
     * @param script The cache key of the script to be compiled/executed.  For dynamic scripts this is the actual
     *               script source code.  For indexed scripts this is the id used in the request.  For on disk scripts
     *               this is the file name.
     * @param type The type of script -- dynamic, indexed, or file.
     * @param params The map of parameters the script will be executed with.
     */
    public Script(String lang, String script, ScriptType type, Map<String, Object> params) {
        if (script == null) {
            throw new ElasticsearchIllegalArgumentException("The parameter script (String) must not be null in Script.");
        }
        if (type == null) {
            throw new ElasticsearchIllegalArgumentException("The parameter type (ScriptType) must not be null in Script.");
        }

        this.lang = lang;
        this.script = script;
        this.type = type;
        this.params = params;
    }

    /**
     * Method for getting language.
     * @return The language of the script to be compiled/executed.
     */
    public String getLang() {
        return lang;
    }

    /**
     * Method for getting the script.
     * @return The cache key of the script to be compiled/executed.  For dynamic scripts this is the actual
     *         script source code.  For indexed scripts this is the id used in the request.  For on disk scripts
     *         this is the file name.
     */
    public String getScript() {
        return script;
    }

    /**
     * Method for getting the type.
     * @return The type of script -- dynamic, indexed, or file.
     */
    public ScriptType getType() {
        return type;
    }

    /**
     * Method for getting the parameters.
     * @return The map of parameters the script will be executed with.
     */
    public Map<String, Object> getParams() {
        return params;
    }
}
