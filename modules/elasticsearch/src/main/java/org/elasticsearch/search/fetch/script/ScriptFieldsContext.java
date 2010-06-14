/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.search.fetch.script;

import org.elasticsearch.index.field.function.script.ScriptFieldsFunction;

import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptFieldsContext {

    public static class ScriptField {
        private final String name;
        private final ScriptFieldsFunction scriptFieldsFunction;
        private final Map<String, Object> params;

        public ScriptField(String name, ScriptFieldsFunction scriptFieldsFunction, Map<String, Object> params) {
            this.name = name;
            this.scriptFieldsFunction = scriptFieldsFunction;
            this.params = params;
        }

        public String name() {
            return name;
        }

        public ScriptFieldsFunction scriptFieldsFunction() {
            return scriptFieldsFunction;
        }

        public Map<String, Object> params() {
            return params;
        }
    }

    private List<ScriptField> fields;

    public ScriptFieldsContext(List<ScriptField> fields) {
        this.fields = fields;
    }

    public List<ScriptField> fields() {
        return this.fields;
    }
}
