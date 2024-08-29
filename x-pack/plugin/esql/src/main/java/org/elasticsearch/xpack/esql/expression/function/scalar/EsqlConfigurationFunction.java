/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Objects;

public abstract class EsqlConfigurationFunction extends EsqlScalarFunction {

    private final Configuration configuration;

    protected EsqlConfigurationFunction(Source source, List<Expression> fields, Configuration configuration) {
        super(source, fields);
        this.configuration = configuration;
    }

    public Configuration configuration() {
        return configuration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), configuration);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        EsqlConfigurationFunction other = (EsqlConfigurationFunction) obj;

        return configuration.equals(other.configuration);
    }
}
