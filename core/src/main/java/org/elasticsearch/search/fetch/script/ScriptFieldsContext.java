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

package org.elasticsearch.search.fetch.script;

import org.elasticsearch.script.SearchScript;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ScriptFieldsContext {

    public static class ScriptField {
        private final String name;
        private final SearchScript script;
        private final boolean ignoreException;

        public ScriptField(String name, SearchScript script, boolean ignoreException) {
            this.name = name;
            this.script = script;
            this.ignoreException = ignoreException;
        }

        public String name() {
            return name;
        }

        public SearchScript script() {
            return this.script;
        }

        public boolean ignoreException() {
            return ignoreException;
        }
    }

    private List<ScriptField> fields = new ArrayList<>();

    public ScriptFieldsContext() {
    }

    public void add(ScriptField field) {
        this.fields.add(field);
    }

    public List<ScriptField> fields() {
        return this.fields;
    }
}
