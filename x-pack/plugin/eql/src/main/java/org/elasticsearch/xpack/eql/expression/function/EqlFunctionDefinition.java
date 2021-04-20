/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function;

import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;

import java.util.List;

/**
 * Dedicated wrapper for EQL specific function definitions.
 * Used mainly for validating case sensitivity of wrapping functions.
 */
class EqlFunctionDefinition extends FunctionDefinition {

    private final boolean caseAware;

    protected EqlFunctionDefinition(String name,
                                    List<String> aliases,
                                    Class<? extends Function> clazz,
                                    boolean caseAware,
                                    Builder builder) {
        super(name, aliases, clazz, builder);
        this.caseAware = caseAware;
    }

    public boolean isCaseAware() {
        return caseAware;
    }

    @Override
    protected Builder builder() {
        return super.builder();
    }
}
