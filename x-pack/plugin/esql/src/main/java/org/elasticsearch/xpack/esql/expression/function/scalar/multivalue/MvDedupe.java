/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.MultivalueDedupe;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Removes duplicate values from a multivalued field.
 */
public class MvDedupe extends AbstractMultivalueFunction {
    @FunctionInfo(returnType = "?", description = "Remove duplicate values from a multivalued field.")
    public MvDedupe(
        Source source,
        @Param(
            name = "v",
            type = { "boolean", "date", "double", "ip", "text", "integer", "keyword", "version", "long" }  // TODO add unsigned_long
        ) Expression field
    ) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), EsqlDataTypes::isRepresentable, sourceText(), null, "representable");
    }

    @Override
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        return MultivalueDedupe.evaluator(LocalExecutionPlanner.toElementType(dataType()), fieldEval);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvDedupe(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvDedupe::new, field());
    }
}
