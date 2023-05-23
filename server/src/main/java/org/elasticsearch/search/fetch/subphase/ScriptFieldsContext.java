/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.script.FieldScript;

import java.util.ArrayList;
import java.util.List;

public class ScriptFieldsContext {

    public static class ScriptField {
        private final String name;
        private final FieldScript.LeafFactory script;
        private final boolean ignoreException;

        public ScriptField(String name, FieldScript.LeafFactory script, boolean ignoreException) {
            this.name = name;
            this.script = script;
            this.ignoreException = ignoreException;
        }

        public String name() {
            return name;
        }

        public FieldScript.LeafFactory script() {
            return this.script;
        }

        public boolean ignoreException() {
            return ignoreException;
        }
    }

    private final List<ScriptField> fields = new ArrayList<>();

    public ScriptFieldsContext() {}

    public void add(ScriptField field) {
        this.fields.add(field);
    }

    public List<ScriptField> fields() {
        return this.fields;
    }
}
