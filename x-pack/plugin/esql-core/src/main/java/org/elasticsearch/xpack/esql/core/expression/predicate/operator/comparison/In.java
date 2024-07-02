/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Foldables;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.ordinal;

public class In extends ScalarFunction {

    private final Expression value;
    private final List<Expression> list;
    private final ZoneId zoneId;

    public In(Source source, Expression value, List<Expression> list) {
        this(source, value, list, null);
    }

    public In(Source source, Expression value, List<Expression> list, ZoneId zoneId) {
        super(source, CollectionUtils.combine(list, value));
        this.value = value;
        this.list = new ArrayList<>(new LinkedHashSet<>(list));
        this.zoneId = zoneId;
    }

    @Override
    protected NodeInfo<In> info() {
        return NodeInfo.create(this, In::new, value(), list(), zoneId());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new In(source(), newChildren.get(newChildren.size() - 1), newChildren.subList(0, newChildren.size() - 1), zoneId());
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    public Expression value() {
        return value;
    }

    public List<Expression> list() {
        return list;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children()) || (Expressions.foldable(list) && list().stream().allMatch(Expressions::isNull));
    }

    @Override
    public Boolean fold() {
        // Optimization for early return and Query folding to LocalExec
        if (Expressions.isNull(value) || list.size() == 1 && Expressions.isNull(list.get(0))) {
            return null;
        }
        return apply(value.fold(), foldAndConvertListOfValues(list, value.dataType()));
    }

    private static Boolean apply(Object input, List<Object> values) {
        Boolean result = Boolean.FALSE;
        for (Object v : values) {
            Boolean compResult = Comparisons.eq(input, v);
            if (compResult == null) {
                result = null;
            } else if (compResult == Boolean.TRUE) {
                return Boolean.TRUE;
            }
        }
        return result;
    }

    @Override
    protected Expression canonicalize() {
        // order values for commutative operators
        List<Expression> canonicalValues = Expressions.canonicalize(list);
        Collections.sort(canonicalValues, (l, r) -> Integer.compare(l.hashCode(), r.hashCode()));
        return new In(source(), value, canonicalValues, zoneId);
    }

    protected List<Object> foldAndConvertListOfValues(List<Expression> expressions, DataType dataType) {
        List<Object> values = new ArrayList<>(expressions.size());
        for (Expression e : expressions) {
            values.add(DataTypeConverter.convert(Foldables.valueOf(e), dataType));
        }
        return values;
    }

    protected boolean areCompatible(DataType left, DataType right) {
        return DataType.areCompatible(left, right);
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = TypeResolutions.isExact(value, functionName(), DEFAULT);
        if (resolution.unresolved()) {
            return resolution;
        }

        for (Expression ex : list) {
            if (ex.foldable() == false) {
                return new TypeResolution(
                    format(
                        null,
                        "Comparisons against fields are not (currently) supported; offender [{}] in [{}]",
                        Expressions.name(ex),
                        sourceText()
                    )
                );
            }
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

        return super.resolveType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, list);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        In other = (In) obj;
        return Objects.equals(value, other.value) && Objects.equals(list, other.list);
    }
}
