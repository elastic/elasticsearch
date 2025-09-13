/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ColumnsBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.AtomType;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.type.AtomType.OBJECT;

public class Get extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Get", Get::new);

    private final Expression columns;
    private final Expression nameExpression;

    private String name;
    private DataType targetType;

    @FunctionInfo(
        returnType = { "any" },
        description = "NOCOMMIT",
        examples = {} // NOCOMMIT
    )
    public Get(
        Source source,
        @Param(name = "object", type = { "object" }, description = "NOCOMMIT") Expression columns,
        @Param(name = "name", type = { "keyword" }, description = "Name to lookup. Must be a constant.") Expression name
    ) {
        super(source, List.of(columns, name));
        this.columns = columns;
        this.nameExpression = name;
    }

    private Get(StreamInput in) throws IOException {
        super(Source.readFrom((StreamInput & PlanStreamInput) in));
        this.columns = in.readNamedWriteable(Expression.class);
        this.nameExpression = in.readNamedWriteable(Expression.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new IOException("NOCOMMIT GET");
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (columns.dataType().atom() != OBJECT) {
            // NOCOMMIT what a terrible name to show to users.
            return new TypeResolution("[column] first argument must be an object function");
        }
        {
            if (nameExpression instanceof Literal l && l.value() instanceof BytesRef b) {
                name = b.utf8ToString();
            } else {
                return new TypeResolution("[column] second argument must be a constant string");
            }
        }
        {
            if (targetTypeExpression instanceof Literal l && l.value() instanceof BytesRef b) {
                String t = b.utf8ToString().toUpperCase(Locale.ROOT);
                switch (t) {
                    case "KEYWORD":
                        targetType = DataType.KEYWORD;
                        break;
                    case "INT":
                        targetType = DataType.INTEGER;
                        break;
                    default:
                        return new TypeResolution("[column] third argument must be one of [KEYWORD, INT] but was " + t);
                }
            } else {
                return new TypeResolution("[column] third argument must be a constant string");
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        resolveType();
        return targetType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Get(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Get::new, columns, nameExpression, targetTypeExpression);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        resolveType();
        EvalOperator.ExpressionEvaluator.Factory columns = toEvaluator.apply(this.columns);
        return new EvaluatorFactory(source(), columns, name, targetType);
    }

    static class EvaluatorFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory columns;
        private final String name;
        private final DataType targetType;

        EvaluatorFactory(Source source, EvalOperator.ExpressionEvaluator.Factory columns, String name, DataType targetType) {
            this.source = source;
            this.columns = columns;
            this.name = name;
            this.targetType = targetType;
        }

        @Override
        public Evaluator get(DriverContext context) {
            return new Evaluator(source, columns.get(context), name, targetType, context);
        }

        @Override
        public String toString() {
            return "Columns[columns=" + columns + ", name=" + name + ", targetType=" + targetType + "]";
        }
    }

    static class Evaluator implements EvalOperator.ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final Source source;

        private final EvalOperator.ExpressionEvaluator columns;
        private final String name;
        private final DataType targetType;

        private final DriverContext driverContext;

        private Warnings warnings;

        Evaluator(Source source, EvalOperator.ExpressionEvaluator columns, String name, DataType targetType, DriverContext driverContext) {
            this.source = source;
            this.columns = columns;
            this.name = name;
            this.targetType = targetType;
            this.driverContext = driverContext;
        }

        @Override
        public Block eval(Page page) {
            try (ColumnsBlock valBlock = (ColumnsBlock) columns.eval(page)) {
                ColumnsBlock.RuntimeTypedBlock uncast = valBlock.columns().get(name);
                if (uncast == null) {
                    return nulls(page);
                }
                DataType runtimeType = (DataType) uncast.type();
                if (targetType == runtimeType) {
                    uncast.block().incRef();
                    return uncast.block();
                }
                warnings().registerException(
                    IllegalArgumentException.class,
                    "runtime type [" + runtimeType + "] incompatible with target type [" + targetType + "]"
                );
                return nulls(page);
            }
        }

        private Block nulls(Page page) {
            return driverContext.blockFactory().newConstantNullBlock(page.getPositionCount());
        }

        @Override
        public long baseRamBytesUsed() {
            long baseRamBytesUsed = BASE_RAM_BYTES_USED;
            baseRamBytesUsed += columns.baseRamBytesUsed();
            // NOCOMMIT bytes for name and type
            return baseRamBytesUsed;
        }

        @Override
        public void close() {
            columns.close();
        }

        @Override
        public String toString() {
            return "NOCOMMIT";
        }

        Warnings warnings() {
            if (warnings == null) {
                this.warnings = Warnings.createWarnings(
                    driverContext.warningsMode(),
                    source.source().getLineNumber(),
                    source.source().getColumnNumber(),
                    source.text()
                );
            }
            return warnings;
        }
    }
}
