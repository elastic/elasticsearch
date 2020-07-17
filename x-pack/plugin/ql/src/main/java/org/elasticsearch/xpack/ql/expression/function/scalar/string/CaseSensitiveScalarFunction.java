/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.ConfigurationFunction;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public abstract class CaseSensitiveScalarFunction extends ConfigurationFunction {

    protected CaseSensitiveScalarFunction(Source source, List<Expression> fields, Configuration configuration) {
        super(source, fields, configuration);
    }

    public abstract boolean isCaseSensitive();

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isCaseSensitive());
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other) && Objects.equals(((CaseSensitiveScalarFunction) other).isCaseSensitive(), isCaseSensitive());
    }
}
