/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.ql.expression.function.DefaultFunctionTypeRegistry;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.grouping.GroupingFunction;

public class SqlFunctionTypeRegistry extends DefaultFunctionTypeRegistry {

    public static final SqlFunctionTypeRegistry INSTANCE = new SqlFunctionTypeRegistry();

    private enum Types {
        GROUPING(GroupingFunction.class),
        SCORE(Score.class);

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
        return super.type(clazz);
    }

}
