/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.Comparisons;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.ordinal;

public class In extends EsqlScalarFunction {
    private final Expression value;
    private final List<Expression> list;

    @FunctionInfo(
        returnType = "boolean",
        description = "The `IN` operator allows testing whether a field or expression equals an element in a list of literals, "
            + "fields or expressions:",
        examples = @Example(file = "row", tag = "in-with-expressions")
    )
    public In(Source source, Expression value, List<Expression> list) {
        super(source, CollectionUtils.combine(list, value));
        this.value = value;
        this.list = list;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, In::new, value, list);
    }

    @Override
    public In replaceChildren(List<Expression> newChildren) {
        return new In(source(), newChildren.get(newChildren.size() - 1), newChildren.subList(0, newChildren.size() - 1));
    }

    @Override
    public boolean foldable() {
        // QL's In fold()s to null, if value() is null, but isn't foldable() unless all children are
        // TODO: update this null check in QL too?
        return Expressions.isNull(value)
            || Expressions.foldable(children())
            || (Expressions.foldable(list) && list.stream().allMatch(Expressions::isNull));
    }

    @Override
    public Object fold() {
        if (Expressions.isNull(value) || list.stream().allMatch(e -> Expressions.isNull(e))) {
            return null;
        }
        return super.fold();
    }

    @Override
    protected Expression canonicalize() {
        // order values for commutative operators
        List<Expression> canonicalValues = Expressions.canonicalize(list);
        Collections.sort(canonicalValues, (l, r) -> Integer.compare(l.hashCode(), r.hashCode()));
        return new In(source(), value, canonicalValues);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        var commonType = commonType();
        EvalOperator.ExpressionEvaluator.Factory lhs;
        EvalOperator.ExpressionEvaluator.Factory[] factories;
        if (commonType.isNumeric()) {
            lhs = Cast.cast(source(), value.dataType(), commonType, toEvaluator.apply(value));
            factories = list.stream()
                .map(e -> Cast.cast(source(), e.dataType(), commonType, toEvaluator.apply(e)))
                .toArray(EvalOperator.ExpressionEvaluator.Factory[]::new);
        } else {
            lhs = toEvaluator.apply(value);
            factories = list.stream().map(e -> toEvaluator.apply(e)).toArray(EvalOperator.ExpressionEvaluator.Factory[]::new);
        }

        if (commonType == BOOLEAN) {
            return new InBooleanEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == DOUBLE) {
            return new InDoubleEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == INTEGER) {
            return new InIntEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == LONG || commonType == DATETIME || commonType == UNSIGNED_LONG) {
            return new InLongEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == KEYWORD
            || commonType == TEXT
            || commonType == IP
            || commonType == VERSION
            || commonType == UNSUPPORTED
            || EsqlDataTypes.isSpatial(commonType)) {
            return new InBytesRefEvaluator.Factory(source(), toEvaluator.apply(value), factories);
        }
        if (commonType == NULL) {
            return EvalOperator.CONSTANT_NULL_FACTORY;
        }
        throw EsqlIllegalArgumentException.illegalDataType(commonType);
    }

    private DataType commonType() {
        DataType commonType = value.dataType();
        for (Expression e : list) {
            if (e.dataType() == NULL && value.dataType() != NULL) {
                continue;
            }
            if (EsqlDataTypes.isSpatial(commonType)) {
                if (e.dataType() == commonType) {
                    continue;
                } else {
                    commonType = NULL;
                    break;
                }
            }
            commonType = EsqlDataTypeRegistry.INSTANCE.commonType(commonType, e.dataType());
        }
        return commonType;
    }

    protected boolean areCompatible(DataType left, DataType right) {
        if (left == UNSIGNED_LONG || right == UNSIGNED_LONG) {
            // automatic numerical conversions not applicable for UNSIGNED_LONG, see Verifier#validateUnsignedLongOperator().
            return left == right;
        }
        if (EsqlDataTypes.isSpatial(left) && EsqlDataTypes.isSpatial(right)) {
            return left == right;
        }
        return EsqlDataTypes.areCompatible(left, right);
    }

    @Override
    protected TypeResolution resolveType() { // TODO: move the foldability check from QL's In to SQL's and remove this method
        TypeResolution resolution = EsqlTypeResolutions.isExact(value, functionName(), DEFAULT);
        if (resolution.unresolved()) {
            return resolution;
        }

        DataType dt = value.dataType();
        for (int i = 0; i < list.size(); i++) {
            Expression listValue = list.get(i);
            if (areCompatible(dt, listValue.dataType()) == false) {
                return new TypeResolution(
                    format(
                        null,
                        "{} argument of [{}] must be [{}], found value [{}] type [{}]",
                        ordinal(i + 1),
                        sourceText(),
                        dt.typeName(),
                        Expressions.name(listValue),
                        listValue.dataType().typeName()
                    )
                );
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public Expression value() {
        return value;
    }

    public List<Expression> list() {
        return list;
    }

    @Override
    public DataType dataType() {
        return BOOLEAN;
    }

    private static <T> void processCommon(BooleanBlock.Builder builder, BitSet nulls, T lhs, T[] rhs) {
        boolean hasNull = nulls.cardinality() > 0;
        for (int i = 0; i < rhs.length; i++) {
            if (nulls.get(i)) {
                continue;
            }
            Boolean compResult = Comparisons.eq(lhs, rhs[i]);
            if (compResult == Boolean.TRUE) {
                builder.appendBoolean(true);
                return;
            }
        }
        if (hasNull) {
            builder.appendNull();
        } else {
            builder.appendBoolean(false);
        }
    }

    private static void checkMV(int p, Block field) {
        if (field.getValueCount(p) > 1) {
            throw new IllegalArgumentException("single-value function encountered multi-value");
        }
    }

    @Evaluator(extraName = "Boolean", warnExceptions = { IllegalArgumentException.class })
    static void process(BooleanBlock.Builder builder, int p, BooleanBlock lhs, BooleanBlock[] rhs) {
        checkMV(p, lhs);
        Boolean l = lhs.getBoolean(lhs.getFirstValueIndex(p));
        Boolean[] r = new Boolean[rhs.length];
        BitSet nulls = new BitSet(rhs.length);
        int index;
        for (int i = 0; i < rhs.length; i++) {
            checkMV(p, rhs[i]);
            if (rhs[i].isNull(p)) {
                nulls.set(i);
                continue;
            }
            index = rhs[i].getFirstValueIndex(p);
            r[i] = rhs[i].getBoolean(index);
        }
        processCommon(builder, nulls, l, r);
    }

    @Evaluator(extraName = "Int", warnExceptions = { IllegalArgumentException.class })
    static void process(BooleanBlock.Builder builder, int p, IntBlock lhs, IntBlock[] rhs) {
        checkMV(p, lhs);
        Integer l = lhs.getInt(lhs.getFirstValueIndex(p));
        Integer[] r = new Integer[rhs.length];
        BitSet nulls = new BitSet(rhs.length);
        int index;
        for (int i = 0; i < rhs.length; i++) {
            checkMV(p, rhs[i]);
            if (rhs[i].isNull(p)) {
                nulls.set(i);
                continue;
            }
            index = rhs[i].getFirstValueIndex(p);
            r[i] = rhs[i].getInt(index);
        }
        processCommon(builder, nulls, l, r);
    }

    @Evaluator(extraName = "Long", warnExceptions = { IllegalArgumentException.class })
    static void process(BooleanBlock.Builder builder, int p, LongBlock lhs, LongBlock[] rhs) {
        checkMV(p, lhs);
        Long l = lhs.getLong(lhs.getFirstValueIndex(p));
        Long[] r = new Long[rhs.length];
        BitSet nulls = new BitSet(rhs.length);
        int index;
        for (int i = 0; i < rhs.length; i++) {
            checkMV(p, rhs[i]);
            if (rhs[i].isNull(p)) {
                nulls.set(i);
                continue;
            }
            index = rhs[i].getFirstValueIndex(p);
            r[i] = rhs[i].getLong(index);
        }
        processCommon(builder, nulls, l, r);
    }

    @Evaluator(extraName = "Double", warnExceptions = { IllegalArgumentException.class })
    static void process(BooleanBlock.Builder builder, int p, DoubleBlock lhs, DoubleBlock[] rhs) {
        checkMV(p, lhs);
        Double l = lhs.getDouble(lhs.getFirstValueIndex(p));
        Double[] r = new Double[rhs.length];
        BitSet nulls = new BitSet(rhs.length);
        int index;
        for (int i = 0; i < rhs.length; i++) {
            checkMV(p, rhs[i]);
            if (rhs[i].isNull(p)) {
                nulls.set(i);
                continue;
            }
            index = rhs[i].getFirstValueIndex(p);
            r[i] = rhs[i].getDouble(index);
        }
        processCommon(builder, nulls, l, r);
    }

    @Evaluator(extraName = "BytesRef", warnExceptions = { IllegalArgumentException.class })
    static void process(BooleanBlock.Builder builder, int p, BytesRefBlock lhs, BytesRefBlock[] rhs) {
        checkMV(p, lhs);
        BytesRef lhsScratch = new BytesRef();
        BytesRef l = lhs.getBytesRef(lhs.getFirstValueIndex(p), lhsScratch);
        BytesRef[] r = new BytesRef[rhs.length];
        BytesRef[] rhsScratch = new BytesRef[rhs.length];
        BitSet nulls = new BitSet(rhs.length);
        int index;
        for (int i = 0; i < rhs.length; i++) {
            checkMV(p, rhs[i]);
            if (rhs[i].isNull(p)) {
                nulls.set(i);
                continue;
            }
            rhsScratch[i] = new BytesRef();
            index = rhs[i].getFirstValueIndex(p);
            r[i] = rhs[i].getBytesRef(index, rhsScratch[i]);
        }
        processCommon(builder, nulls, l, r);
    }
}
