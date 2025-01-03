/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;

class TypeConverter {
    private final String evaluatorName;
    private final ExpressionEvaluator convertEvaluator;

    private TypeConverter(String evaluatorName, ExpressionEvaluator convertEvaluator) {
        this.evaluatorName = evaluatorName;
        this.convertEvaluator = convertEvaluator;
    }

    public static TypeConverter fromConvertFunction(AbstractConvertFunction convertFunction) {
        DriverContext driverContext1 = new DriverContext(
            BigArrays.NON_RECYCLING_INSTANCE,
            new org.elasticsearch.compute.data.BlockFactory(
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                BigArrays.NON_RECYCLING_INSTANCE
            )
        );
        return new TypeConverter(convertFunction.functionName(), convertFunction.toEvaluator(new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                return driverContext -> new ExpressionEvaluator() {
                    @Override
                    public org.elasticsearch.compute.data.Block eval(Page page) {
                        // This is a pass-through evaluator, since it sits directly on the source loading (no prior expressions)
                        return page.getBlock(0);
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public FoldContext foldCtx() {
                throw new IllegalStateException("not folding");
            }
        }).get(driverContext1));
    }

    public Block convert(Block block) {
        return convertEvaluator.eval(new Page(block));
    }

    @Override
    public String toString() {
        return evaluatorName;
    }
}
