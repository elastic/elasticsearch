/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;

import java.util.List;

class SqlFunctionDefinition extends FunctionDefinition {

    /**
     * Is this a datetime function compatible with {@code EXTRACT}.
     */
    private final boolean extractViable;

    SqlFunctionDefinition(String name, List<String> aliases, Class<? extends Function> clazz, boolean dateTime, Builder builder) {
        super(name, aliases, clazz, builder);
        this.extractViable = dateTime;
    }

    /**
     * Is this a datetime function compatible with {@code EXTRACT}.
     */
    public boolean extractViable() {
        return extractViable;
    }

    @Override
    protected Builder builder() {
        return super.builder();
    }
}
