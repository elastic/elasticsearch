/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.singletonList;

public abstract class UnaryScalarFunction extends ScalarFunction {

    private final Expression field;

    protected UnaryScalarFunction(Location location) {
        super(location);
        this.field = null;
    }

    protected UnaryScalarFunction(Location location, Expression field) {
        super(location, singletonList(field));
        this.field = field;
    }

    public Expression field() {
        return field;
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Override
    public ScriptTemplate asScript() {
        return asScript(field);
    }
}