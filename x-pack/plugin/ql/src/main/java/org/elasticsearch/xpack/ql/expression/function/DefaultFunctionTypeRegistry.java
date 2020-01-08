/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.ConditionalFunction;


public class DefaultFunctionTypeRegistry implements FunctionTypeRegistry {

    public static final DefaultFunctionTypeRegistry INSTANCE = new DefaultFunctionTypeRegistry();

    private enum Types {
        AGGREGATE(AggregateFunction.class),
        CONDITIONAL(ConditionalFunction.class),
        SCALAR(ScalarFunction.class);

        private Class<? extends Function> baseClass;

        Types(Class<? extends Function> base) {
            this.baseClass = base;
        }
    }

    @Override
    public String type(Class<? extends Function> clazz) {
        for (Types type : Types.values()) {
            if (type.baseClass.isAssignableFrom(clazz)) {
                return type.name();
            }
        }
        return "UNKNOWN";
    }
}
