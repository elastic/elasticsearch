/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;

public class ScriptQuery extends LeafQuery {

    private final ScriptTemplate script;

    @SuppressWarnings("this-escape")
    public ScriptQuery(Source source, ScriptTemplate script) {
        super(source);
        // make script null safe
        this.script = nullSafeScript(script);
    }

    public ScriptTemplate script() {
        return script;
    }

    @Override
    public QueryBuilder asBuilder() {
        return scriptQuery(script.toPainless());
    }

    @Override
    public int hashCode() {
        return Objects.hash(script);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ScriptQuery other = (ScriptQuery) obj;
        return Objects.equals(script, other.script);
    }

    @Override
    protected String innerToString() {
        return script.toString();
    }

    protected ScriptTemplate nullSafeScript(ScriptTemplate scriptTemplate) {
        return Scripts.nullSafeFilter(scriptTemplate);
    }
}
