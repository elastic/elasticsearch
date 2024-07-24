/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;

import java.io.IOException;
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
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "In", In::new);

    private final Expression value;
    private final List<Expression> list;

    @FunctionInfo(
        returnType = "boolean",
        description = "The `IN` operator allows testing whether a field or expression equals an element in a list of literals, "
            + "fields or expressions.",
        examples = @Example(file = "row", tag = "in-with-expressions")
    )
    public In(
        Source source,
        @Param(
            name = "field",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" },
            description = "An expression."
        ) Expression value,
        @Param(
            name = "inlist",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" },
            description = "A list of items."
        ) List<Expression> list
    ) {
        super(source, CollectionUtils.combine(list, value));
        this.value = value;
        this.list = list;
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

    private In(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(value);
        out.writeNamedWriteableCollection(list);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, In::new, value, list);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
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
        if (Expressions.isNull(value) || list.stream().allMatch(Expressions::isNull)) {
            return null;
        }
        return super.fold();
    }

    protected boolean areCompatible(DataType left, DataType right) {
        if (left == UNSIGNED_LONG || right == UNSIGNED_LONG) {
            // automatic numerical conversions not applicable for UNSIGNED_LONG, see Verifier#validateUnsignedLongOperator().
            return left == right;
        }
        if (DataType.isSpatial(left) && DataType.isSpatial(right)) {
            return left == right;
        }
        return DataType.areCompatible(left, right);
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
            || DataType.isSpatial(commonType)) {
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
            if (DataType.isSpatial(commonType)) {
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

    static boolean process(BitSet nulls, BitSet mvs, int lhs, int[] rhs) {
        for (int i = 0; i < rhs.length; i++) {
            if ((nulls != null && nulls.get(i)) || (mvs != null && mvs.get(i))) {
                continue;
            }
            Boolean compResult = Comparisons.eq(lhs, rhs[i]);
            if (compResult == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }

    static boolean process(BitSet nulls, BitSet mvs, long lhs, long[] rhs) {
        for (int i = 0; i < rhs.length; i++) {
            if ((nulls != null && nulls.get(i)) || (mvs != null && mvs.get(i))) {
                continue;
            }
            Boolean compResult = Comparisons.eq(lhs, rhs[i]);
            if (compResult == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }

    static boolean process(BitSet nulls, BitSet mvs, double lhs, double[] rhs) {
        for (int i = 0; i < rhs.length; i++) {
            if ((nulls != null && nulls.get(i)) || (mvs != null && mvs.get(i))) {
                continue;
            }
            Boolean compResult = Comparisons.eq(lhs, rhs[i]);
            if (compResult == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }

    static boolean process(BitSet nulls, BitSet mvs, BytesRef lhs, BytesRef[] rhs) {
        for (int i = 0; i < rhs.length; i++) {
            if ((nulls != null && nulls.get(i)) || (mvs != null && mvs.get(i))) {
                continue;
            }
            Boolean compResult = Comparisons.eq(lhs, rhs[i]);
            if (compResult == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }
}
