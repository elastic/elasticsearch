/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.querydsl.container;

import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;

import java.util.Objects;

public class ScriptSort extends Sort {

    private final ScriptTemplate script;

    public ScriptSort(ScriptTemplate script, Direction direction, Missing missing) {
        super(direction, missing);
        this.script = Scripts.nullSafeSort(script);
    }

    public ScriptTemplate script() {
        return script;
    }

    @Override
    public int hashCode() {
        return Objects.hash(direction(), missing(), script);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        ScriptSort other = (ScriptSort) obj;
        return Objects.equals(direction(), other.direction())
                && Objects.equals(missing(), other.missing())
                && Objects.equals(script, other.script);
    }
}
