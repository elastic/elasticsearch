/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.util.StringUtils.ordinal;

public class In extends org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In {
    public In(Source source, Expression value, List<Expression> list) {
        super(source, value, list);
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In> info() {
        return NodeInfo.create(this, In::new, value(), list());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new In(source(), newChildren.get(newChildren.size() - 1), newChildren.subList(0, newChildren.size() - 1));
    }

    @Override
    public boolean foldable() {
        // QL's In fold()s to null, if value() is null, but isn't foldable() unless all children are
        // TODO: update this null check in QL too?
        return Expressions.isNull(value()) || super.foldable();
    }

    @Override
    protected boolean areCompatible(DataType left, DataType right) {
        return EsqlDataTypes.areCompatible(left, right);
    }

    @Override
    protected TypeResolution resolveType() { // TODO: move the foldability check from QL's In to SQL's and remove this method
        TypeResolution resolution = TypeResolutions.isExact(value(), functionName(), DEFAULT);
        if (resolution.unresolved()) {
            return resolution;
        }

        DataType dt = value().dataType();
        for (int i = 0; i < list().size(); i++) {
            Expression listValue = list().get(i);
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
}
