/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public abstract class ConfigurationAggregateFunction extends AggregateFunction {

    private final Configuration configuration;

    ConfigurationAggregateFunction(Source source, Expression field, List<Expression> parameters, Configuration configuration) {
        super(source, field, parameters);
        this.configuration = configuration;
    }

    ConfigurationAggregateFunction(
        Source source,
        Expression field,
        Expression filter,
        List<Expression> parameters,
        Configuration configuration
    ) {
        super(source, field, filter, parameters);
        this.configuration = configuration;
    }

    ConfigurationAggregateFunction(Source source, Expression field, Configuration configuration) {
        super(source, field);
        this.configuration = configuration;
    }

    ConfigurationAggregateFunction(StreamInput in) throws IOException {
        super(in);
        this.configuration = ((PlanStreamInput) in).configuration();
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
        ConfigurationAggregateFunction other = (ConfigurationAggregateFunction) obj;

        return configuration.equals(other.configuration);
    }
}
