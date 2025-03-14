/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isWholeNumber;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Calculates the geohash of geo_point geometries.
 */
public class StGeohash extends UnarySpatialBinaryFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StGeohash",
        StGeohash::new
    );

    @FunctionInfo(
        returnType = "keyword",
        description = "Calculates the `geohash` of the supplied geo_point at the specified precision.",
        examples = @Example(file = "spatial-grid", tag = "st_geohash-grid")
    )
    public StGeohash(
        Source source,
        @Param(
            name = "point",
            type = { "geo_point" },
            description = "Expression of type `geo_point`. If `null`, the function returns `null`."
        ) Expression field,
        @Param(
            name = "precision",
            type = { "integer" },
            description = "Expression of type `integer`. If `null`, the function returns `null`."
        ) Expression precision
    ) {
        this(source, field, precision, false);
    }

    private StGeohash(Source source, Expression field, Expression precision, boolean spatialDocValues) {
        super(source, field, precision, spatialDocValues);
    }

    private StGeohash(StreamInput in) throws IOException {
        super(in, false);
    }

    @Override
    public UnarySpatialBinaryFunction withDocValues(boolean useDocValues) {
        // Only update the docValues flags if the field is found in the attributes
        boolean leftDV = this.spatialDocsValues || useDocValues;
        return new StGeohash(source(), left(), right(), leftDV);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new StGeohash(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeohash::new, left(), right());
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isGeoPoint(left(), sourceText());
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isWholeNumber(right(), sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    protected static Expression.TypeResolution isGeoPoint(Expression e, String operationName) {

        return isType(e, t -> t.equals(GEO_POINT), operationName, FIRST, GEO_POINT.typeName());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // TODO: Implement
        if (left().foldable()) {
            // Assume right is not foldable, since that would be dealt with in isFoldable() and fold()
            var point = (BytesRef) left().fold(toEvaluator.foldCtx());
            return new StGeohashFromLiteralAndFieldEvaluator.Factory(source(), point, toEvaluator.apply(right()));
        } else if (right().foldable()) {
            // Assume left is not foldable, since that would be dealt with in isFoldable() and fold()
            int precision = (int) right().fold(toEvaluator.foldCtx());
            return spatialDocsValues
                ? new StGeohashFromFieldDocValuesAndLiteralEvaluator.Factory(source(), toEvaluator.apply(left()), precision)
                : new StGeohashFromFieldAndLiteralEvaluator.Factory(source(), toEvaluator.apply(left()), precision);
        } else {
            // Both arguments come from index fields
            return spatialDocsValues
                ? new StGeohashFromFieldDocValuesAndFieldEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()))
                : new StGeohashFromFieldAndFieldEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
        }
    }

    @Override
    public Object fold(FoldContext ctx) {
        var point = (BytesRef) left().fold(ctx);
        int precision = (int) right().fold(ctx);
        return calculateGeohash(GEO.wkbAsPoint(point), precision);
    }

    @Evaluator(extraName = "FromFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromFieldAndLiteral(BytesRef in, @Fixed int precision) {
        return calculateGeohash(GEO.wkbAsPoint(in), precision);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromFieldDocValuesAndLiteral(long encoded, @Fixed int precision) {
        return calculateGeohash(GEO.longAsPoint(encoded), precision);
    }

    @Evaluator(extraName = "FromFieldAndField", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromFieldAndField(BytesRef in, int precision) {
        return calculateGeohash(GEO.wkbAsPoint(in), precision);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndField", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromFieldDocValuesAndField(long encoded, int precision) {
        return calculateGeohash(GEO.longAsPoint(encoded), precision);
    }

    @Evaluator(extraName = "FromLiteralAndField", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromLiteralAndField(@Fixed BytesRef in, int precision) {
        return calculateGeohash(GEO.wkbAsPoint(in), precision);
    }

    protected static BytesRef calculateGeohash(Point point, int precision) {
        return new BytesRef(Geohash.stringEncode(point.getX(), point.getY(), precision));
    }
}
