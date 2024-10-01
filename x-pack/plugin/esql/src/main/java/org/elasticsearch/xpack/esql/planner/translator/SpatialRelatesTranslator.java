/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils;
import org.elasticsearch.xpack.esql.querydsl.query.SpatialRelatesQuery;

public class SpatialRelatesTranslator extends ExpressionTranslator<SpatialRelatesFunction> {

    @Override
    protected Query asQuery(SpatialRelatesFunction bc, TranslatorHandler handler) {
        return doTranslate(bc, handler);
    }

    public static void checkSpatialRelatesFunction(Expression constantExpression, ShapeRelation queryRelation) {
        Check.isTrue(
            constantExpression.foldable(),
            "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [ST_{}]",
            constantExpression.sourceLocation().getLineNumber(),
            constantExpression.sourceLocation().getColumnNumber(),
            Expressions.name(constantExpression),
            queryRelation
        );
    }

    public static Query doTranslate(SpatialRelatesFunction bc, TranslatorHandler handler) {
        if (bc.left().foldable()) {
            checkSpatialRelatesFunction(bc.left(), bc.queryRelation());
            return translate(bc, handler, bc.right(), bc.left());
        } else {
            checkSpatialRelatesFunction(bc.right(), bc.queryRelation());
            return translate(bc, handler, bc.left(), bc.right());
        }
    }

    static Query translate(
        SpatialRelatesFunction bc,
        TranslatorHandler handler,
        Expression spatialExpression,
        Expression constantExpression
    ) {
        TypedAttribute attribute = checkIsPushableAttribute(spatialExpression);
        String name = handler.nameOf(attribute);

        try {
            Geometry shape = SpatialRelatesUtils.makeGeometryFromLiteral(constantExpression);
            return new SpatialRelatesQuery(bc.source(), name, bc.queryRelation(), shape, attribute.dataType());
        } catch (IllegalArgumentException e) {
            throw new QlIllegalArgumentException(e.getMessage(), e);
        }
    }
}
