/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public abstract class ConfigurationFunction extends ScalarFunction {

    private final Configuration configuration;

    protected ConfigurationFunction(Source source, List<Expression> fields, Configuration configuration) {
        super(source, fields);
        this.configuration = configuration;
    }

    public Configuration configuration() {
        return configuration;
    }
}
