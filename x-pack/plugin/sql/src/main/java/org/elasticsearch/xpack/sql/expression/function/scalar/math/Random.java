/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Locale;

import static java.lang.String.format;

/**
 * Returns a random double (using the given seed).
 */
public class Random extends MathFunction {

    public Random(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Random> info() {
        return NodeInfo.create(this, Random::new, field());
    }

    @Override
    protected Random replaceChild(Expression newChild) {
        return new Random(location(), newChild);
    }

    @Override
    public String processScript(String template) {
        //TODO: Painless script uses Random since Randomness is not whitelisted
        return super.processScript(
                format(Locale.ROOT, "%s != null ? new Random((long) %s).nextDouble() : Math.random()", template, template));
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.RANDOM;
    }
}
