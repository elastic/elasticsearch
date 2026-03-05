/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.util.Objects;

/**
 * Adapts {@link EsqlScalarFunction} taking a single input into
 * a {@link ValuesSourceReaderOperator.ConverterFactory}.
 */
class TypeConverter implements ValuesSourceReaderOperator.ConverterFactory {
    private final EsqlScalarFunction convertFunction;
    private final ExpressionEvaluator.Factory factory;

    TypeConverter(EsqlScalarFunction convertFunction) {
        this.convertFunction = convertFunction;
        this.factory = convertFunction.toEvaluator(new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                return LOAD_BLOCK_FOR_CONVERSION_FACTORY;
            }

            @Override
            public FoldContext foldCtx() {
                throw new IllegalStateException("not folding");
            }
        });
        // TODO detect a noop conversion
    }

    @Override
    public ValuesSourceReaderOperator.ConverterEvaluator build(DriverContext context) {
        return new Evaluator(factory.get(context));
    }

    private static class Evaluator implements ValuesSourceReaderOperator.ConverterEvaluator {
        private final ExpressionEvaluator evaluator;

        private Evaluator(ExpressionEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        @Override
        public Block convert(Block block) {
            return evaluator.eval(new Page(block));
        }

        @Override
        public void close() {
            evaluator.close();
        }

        @Override
        public String toString() {
            return evaluator.toString();
        }
    }

    @Override
    public String toString() {
        return convertFunction.functionName();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypeConverter that = (TypeConverter) o;
        return Objects.equals(convertFunction, that.convertFunction);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(convertFunction);
    }

    private static final LoadBlockForConversion LOAD_BLOCK_FOR_CONVERSION = new LoadBlockForConversion();
    private static final LoadBlockForConversionFactory LOAD_BLOCK_FOR_CONVERSION_FACTORY = new LoadBlockForConversionFactory();

    private static class LoadBlockForConversion implements ExpressionEvaluator {
        @Override
        public org.elasticsearch.compute.data.Block eval(Page page) {
            // This is a pass-through evaluator, since it sits directly on the source loading (no prior expressions)
            return page.getBlock(0);
        }

        @Override
        public long baseRamBytesUsed() {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return "load";
        }
    }

    private static class LoadBlockForConversionFactory implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return LOAD_BLOCK_FOR_CONVERSION;
        }

        @Override
        public String toString() {
            return "load";
        }
    }
}
