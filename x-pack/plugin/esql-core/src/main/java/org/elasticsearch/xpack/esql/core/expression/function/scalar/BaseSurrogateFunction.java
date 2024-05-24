/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.function.scalar;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

public abstract class BaseSurrogateFunction extends ScalarFunction implements SurrogateFunction {

    private ScalarFunction lazySubstitute;

    public BaseSurrogateFunction(Source source) {
        super(source);
    }

    public BaseSurrogateFunction(Source source, List<Expression> fields) {
        super(source, fields);
    }

    @Override
    public ScalarFunction substitute() {
        if (lazySubstitute == null) {
            lazySubstitute = makeSubstitute();
        }
        return lazySubstitute;
    }

    protected abstract ScalarFunction makeSubstitute();

    @Override
    public boolean foldable() {
        return substitute().foldable();
    }

    @Override
    public Object fold() {
        return substitute().fold();
    }

}
