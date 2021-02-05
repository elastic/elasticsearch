/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.session.Configuration;

public enum SqlFunctionResolution implements FunctionResolutionStrategy {

    /**
     * Behavior of DISTINCT like {@code COUNT DISTINCT(col)}.
     */
    DISTINCT {
        @Override
        public Function buildResolved(UnresolvedFunction uf, Configuration cfg, FunctionDefinition def) {
            if (def instanceof SqlFunctionDefinition) {
                return ((SqlFunctionDefinition) def).builder().build(uf, cfg, true);
            }
            throw new ParsingException(uf.source(), "Cannot use {} on non-SQL function {}", name(), def);
        }

        @Override
        public boolean isValidAlternative(FunctionDefinition def) {
            return false; // think about this later.
        }
    },
    /**
     * Behavior of EXTRACT function calls like {@code EXTRACT(DAY FROM col)}.
     */
    EXTRACT {
        @Override
        public Function buildResolved(UnresolvedFunction uf, Configuration cfg, FunctionDefinition def) {
            if (isValidAlternative(def)) {
                return ((SqlFunctionDefinition) def).builder().build(uf, cfg);
            }
            return uf.withMessage("Invalid datetime field [" + uf.name() + "]. Use any datetime function.");
        }

        @Override
        public boolean isValidAlternative(FunctionDefinition def) {
            return (def instanceof SqlFunctionDefinition) && ((SqlFunctionDefinition) def).extractViable();
        }

        @Override
        public String kind() {
            return "datetime field";
        }
    }
}
