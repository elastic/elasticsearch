/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;

public class ScriptFieldRef extends FieldReference {

    private final String name;
    private final ScriptTemplate script;

    public ScriptFieldRef(String name, ScriptTemplate script) {
        this.name = name;
        this.script = script;
    }

    @Override
    public String name() {
        return name;
    }

    public ScriptTemplate script() {
        return script;
    }

    @Override
    public void collectFields(SqlSourceBuilder sourceBuilder) {
        sourceBuilder.addScriptField(name, script.toPainless());
    }

    @Override
    public String toString() {
        return "{" + name + "}";
    }
}
