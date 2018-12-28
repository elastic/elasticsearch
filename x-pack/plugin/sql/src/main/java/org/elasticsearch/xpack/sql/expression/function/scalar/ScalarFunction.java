/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptWeaver;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * A {@code ScalarFunction} is a {@code Function} that takes values from some
 * operation and converts each to another value. An example would be
 * {@code ABS()}, which takes one value at a time, applies a function to the
 * value (abs) and returns a new value.
 */
public abstract class ScalarFunction extends Function implements ScriptWeaver {

    private ScalarFunctionAttribute lazyAttribute = null;

    protected ScalarFunction(Source source) {
        super(source, emptyList());
    }

    protected ScalarFunction(Source source, List<Expression> fields) {
        super(source, fields);
    }

    @Override
    public final ScalarFunctionAttribute toAttribute() {
        if (lazyAttribute == null) {
            lazyAttribute = new ScalarFunctionAttribute(source(), name(), dataType(), id(), functionId(), asScript(), orderBy(),
                asPipe());
        }
        return lazyAttribute;
    }

    // used if the function is monotonic and thus does not have to be computed for ordering purposes
    // null means the script needs to be used; expression means the field/expression to be used instead
    public Expression orderBy() {
        return null;
    }
}