/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isBoolean;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;

public class Iif extends Case implements OptionalArgument {

    public Iif(Source source, Expression condition, Expression thenResult, Expression elseResult) {
        super(source, Arrays.asList(new IfConditional(source, condition, thenResult), elseResult != null ? elseResult : Literal.NULL));
    }

    Iif(Source source, List<Expression> expressions) {
        super(source, expressions);
    }

    @Override
    protected NodeInfo<? extends Iif> info() {
        return NodeInfo.create(this, Iif::new, combine(conditions(), elseResult()));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Iif(source(), newChildren);
    }

    @Override
    protected TypeResolution resolveType() {
        if (conditions().isEmpty()) {
            return TypeResolution.TYPE_RESOLVED;
        }

        TypeResolution conditionTypeResolution = isBoolean(conditions().get(0).condition(), sourceText(), FIRST);
        if (conditionTypeResolution.unresolved()) {
            return conditionTypeResolution;
        }

        DataType resultDataType = conditions().get(0).dataType();
        if (SqlDataTypes.areCompatible(resultDataType, elseResult().dataType()) == false) {
            return new TypeResolution(
                format(
                    null,
                    "third argument of [{}] must be [{}], found value [{}] type [{}]",
                    sourceText(),
                    resultDataType.typeName(),
                    Expressions.name(elseResult()),
                    elseResult().dataType().typeName()
                )
            );
        }
        return TypeResolution.TYPE_RESOLVED;
    }
}
