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
import org.elasticsearch.compute.data.ElementType;
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
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.AtomType.OBJECT;

public class Get extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Get", Get::new);

    private final Expression object;
    private final Expression nameExpression;

    private String name;
    private DataType resultType;

    @FunctionInfo(
        returnType = { "any" },
        description = "NOCOMMIT",
        examples = {} // NOCOMMIT
    )
    public Get(
        Source source,
        @Param(name = "object", type = { "object" }, description = "NOCOMMIT") Expression object,
        @Param(name = "name", type = { "keyword" }, description = "Name to lookup. Must be a constant.") Expression name
    ) {
        super(source, List.of(object, name));
        this.object = object;
        this.nameExpression = name;
    }

    private Get(StreamInput in) throws IOException {
        super(Source.readFrom((StreamInput & PlanStreamInput) in));
        this.object = in.readNamedWriteable(Expression.class);
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

        Map<String, AtomType> fields = object.dataType().fields();
        if (fields == null) {
            return new TypeResolution("[GET] first argument must be an object");
        }
        if (nameExpression instanceof Literal l && l.value() instanceof BytesRef b) {
            name = b.utf8ToString();
        } else {
            return new TypeResolution("[GET] second argument must be a constant string");
        }
        AtomType resultType = fields.get(name);
        if (resultType != null) {
            this.resultType = resultType.type();
        } else {
            return new TypeResolution("[GET] unknown object field [" + name + "]. Must be one of " + fields.keySet() + "]");
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        if (resultType == null) {
            resolveType();
        }
        return resultType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Get(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Get::new, object, nameExpression);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        resolveType();
        EvalOperator.ExpressionEvaluator.Factory object = toEvaluator.apply(this.object);
        return new EvaluatorFactory(object, name, PlannerUtils.toElementType(resultType));
    }

    record EvaluatorFactory(EvalOperator.ExpressionEvaluator.Factory object, String name, ElementType resultType)
        implements
            EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public Evaluator get(DriverContext context) {
            return new Evaluator(object.get(context), name, resultType);
        }

        @Override
        public String toString() {
            return "Columns[columns=" + object + ", name=" + name + ", resultType=" + resultType + "]";
        }
    }

    static class Evaluator implements EvalOperator.ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final EvalOperator.ExpressionEvaluator columns;
        private final String name;
        private final ElementType resultType;

        Evaluator(EvalOperator.ExpressionEvaluator columns, String name, ElementType resultType) {
            this.columns = columns;
            this.name = name;
            this.resultType = resultType;
        }

        @Override
        public Block eval(Page page) {
            try (ColumnsBlock valBlock = (ColumnsBlock) columns.eval(page)) {
                Block block = valBlock.columns().get(name);
                if (block == null) {
                    // NOCOMMIT we validate up front, but should be ok with missing here?
                    throw new IllegalStateException("column not found [" + name + "]");
                }
                if (block.elementType() != resultType) {
                    throw new IllegalStateException("wrong type [" + name + "][" + resultType + "]: " + block);
                }
                block.incRef();
                return block;
            }
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
    }
}
