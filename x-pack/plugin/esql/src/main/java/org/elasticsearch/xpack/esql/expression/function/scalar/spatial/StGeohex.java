/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.h3.H3;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.ArrayList;

import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Calculates the geohex of geo_point geometries.
 */
public class StGeohex extends SpatialGridFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StGeohex", StGeohex::new);

    @FunctionInfo(
        returnType = "long",
        description = """
            Calculates the `geohex`, the H3 cell-id, of the supplied geo_point at the specified precision.
            The result is long encoded. Use ST_GEOHEX_TO_STRING to convert the result to a string.
            Or use ST_GEOHEX_TO_GEOSHAPE to convert either the long or string geohex to a POLYGON geo_shape.""",
        examples = @Example(file = "spatial-grid", tag = "st_geohex-grid")
    )
    public StGeohex(
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
        ) Expression precision,
        @Param(
            name = "bounds",
            type = { "geo_shape", "geo_point" },
            description = "Bounds to filter the grid tiles, either a geo_shape BBOX or an array of two points",
            optional = true
        ) Expression bounds
    ) {
        this(source, field, precision, bounds, false);
    }

    private StGeohex(Source source, Expression field, Expression precision, Expression bounds, boolean spatialDocValues) {
        super(source, field, precision, bounds, spatialDocValues);
    }

    private StGeohex(StreamInput in) throws IOException {
        super(in, false);
    }

    @Override
    public SpatialGridFunction withDocValues(boolean useDocValues) {
        // Only update the docValues flags if the field is found in the attributes
        boolean docValues = this.spatialDocsValues || useDocValues;
        return new StGeohex(source(), spatialField, parameter, bounds, docValues);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return LONG;
    }

    @Override
    protected SpatialGridFunction replaceChildren(Expression newSpatialField, Expression newParameter, Expression newBounds) {
        return new StGeohex(source(), newSpatialField, newParameter, newBounds);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeohex::new, spatialField, parameter, bounds);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (bounds != null) {
            if (bounds.foldable() == false) {
                throw new IllegalArgumentException("bounds must be foldable");
            }
            GeoBoundingBox bbox = asGeoBoundingBox((BytesRef) bounds.fold(toEvaluator.foldCtx()));
            if (spatialField().foldable()) {
                // Assume right is not foldable, since that would be dealt with in isFoldable() and fold()
                var point = (BytesRef) spatialField.fold(toEvaluator.foldCtx());
                return new StGeohexFromLiteralAndFieldAndLiteralEvaluator.Factory(source(), point, toEvaluator.apply(parameter()), bbox);
            } else if (parameter().foldable()) {
                // Assume left is not foldable, since that would be dealt with in isFoldable() and fold()
                int precision = (int) parameter.fold(toEvaluator.foldCtx());
                GeoHexBoundedPredicate bounds = new GeoHexBoundedPredicate(precision, bbox);
                return spatialDocsValues
                    ? new StGeohexFromFieldDocValuesAndLiteralAndLiteralEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField()),
                        bounds
                    )
                    : new StGeohexFromFieldAndLiteralAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), bounds);
            } else {
                // Both arguments come from index fields
                return spatialDocsValues
                    ? new StGeohexFromFieldDocValuesAndFieldAndLiteralEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField),
                        toEvaluator.apply(parameter),
                        bbox
                    )
                    : new StGeohexFromFieldAndFieldAndLiteralEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField),
                        toEvaluator.apply(parameter),
                        bbox
                    );
            }
        } else {
            if (spatialField().foldable()) {
                // Assume right is not foldable, since that would be dealt with in isFoldable() and fold()
                var point = (BytesRef) spatialField.fold(toEvaluator.foldCtx());
                return new StGeohexFromLiteralAndFieldEvaluator.Factory(source(), point, toEvaluator.apply(parameter()));
            } else if (parameter().foldable()) {
                // Assume left is not foldable, since that would be dealt with in isFoldable() and fold()
                int precision = (int) parameter.fold(toEvaluator.foldCtx());
                // TODO: Use GeoHexBoundedPredicate
                return spatialDocsValues
                    ? new StGeohexFromFieldDocValuesAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField()), precision)
                    : new StGeohexFromFieldAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), precision);
            } else {
                // Both arguments come from index fields
                return spatialDocsValues
                    ? new StGeohexFromFieldDocValuesAndFieldEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField),
                        toEvaluator.apply(parameter)
                    )
                    : new StGeohexFromFieldAndFieldEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField),
                        toEvaluator.apply(parameter)
                    );
            }
        }
    }

    @Override
    public Object fold(FoldContext ctx) {
        var point = (BytesRef) spatialField().fold(ctx);
        int precision = (int) parameter().fold(ctx);
        // TODO: Use GeoHexBoundedPredicate
        return calculateGeohex(GEO.wkbAsPoint(point), precision);
    }

    @Evaluator(extraName = "FromFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndLiteral(LongBlock.Builder results, int p, BytesRefBlock wkbBlock, @Fixed int precision) {
        fromWKB(results, p, wkbBlock, precision);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteral(LongBlock.Builder results, int p, LongBlock encoded, @Fixed int precision) {
        fromEncodedLong(results, p, encoded, precision);
    }

    @Evaluator(extraName = "FromFieldAndField", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndField(LongBlock.Builder results, int p, BytesRefBlock in, int precision) {
        fromWKB(results, p, in, precision);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndField", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndField(LongBlock.Builder results, int p, LongBlock encoded, int precision) {
        fromEncodedLong(results, p, encoded, precision);
    }

    @Evaluator(extraName = "FromLiteralAndField", warnExceptions = { IllegalArgumentException.class })
    static long fromLiteralAndField(@Fixed BytesRef in, int precision) {
        return calculateGeohex(GEO.wkbAsPoint(in), precision);
    }

    protected static long calculateGeohex(Point point, int precision) {
        return H3.geoToH3(point.getLat(), point.getLon(), precision);
    }

    @Evaluator(extraName = "FromFieldAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndLiteralAndLiteral(LongBlock.Builder results, int p, BytesRefBlock in, @Fixed GeoHexBoundedPredicate bounds) {
        fromWKB(results, p, in, bounds);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteralAndLiteral(
        LongBlock.Builder results,
        int p,
        LongBlock encoded,
        @Fixed GeoHexBoundedPredicate bounds
    ) {
        fromEncodedLong(results, p, encoded, bounds);
    }

    @Evaluator(extraName = "FromFieldAndFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndFieldAndLiteral(LongBlock.Builder results, int p, BytesRefBlock in, int precision, @Fixed GeoBoundingBox bbox) {
        GeoHexBoundedPredicate bounds = new GeoHexBoundedPredicate(precision, bbox);
        fromWKB(results, p, in, bounds);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndFieldAndLiteral(
        LongBlock.Builder results,
        int p,
        LongBlock encoded,
        int precision,
        @Fixed GeoBoundingBox bbox
    ) {
        GeoHexBoundedPredicate bounds = new GeoHexBoundedPredicate(precision, bbox);
        fromEncodedLong(results, p, encoded, bounds);
    }

    @Evaluator(extraName = "FromLiteralAndFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static long fromLiteralAndFieldAndLiteral(@Fixed BytesRef in, int precision, @Fixed GeoBoundingBox bbox) {
        GeoHexBoundedPredicate bounds = new GeoHexBoundedPredicate(precision, bbox);
        return calculateGeohex(GEO.wkbAsPoint(in), bounds);
    }

    private static void fromWKB(LongBlock.Builder results, int position, BytesRefBlock wkbBlock, int precision) {
        int valueCount = wkbBlock.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final BytesRef scratch = new BytesRef();
            final int firstValueIndex = wkbBlock.getFirstValueIndex(position);
            if (valueCount == 1) {
                results.appendLong(calculateGeohex(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex, scratch)), precision));
            } else {
                results.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    results.appendLong(calculateGeohex(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex + i, scratch)), precision));
                }
                results.endPositionEntry();
            }
        }
    }

    private static void fromEncodedLong(LongBlock.Builder results, int position, LongBlock encoded, int precision) {
        int valueCount = encoded.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final int firstValueIndex = encoded.getFirstValueIndex(position);
            if (valueCount == 1) {
                results.appendLong(calculateGeohex(GEO.longAsPoint(encoded.getLong(firstValueIndex)), precision));
            } else {
                results.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    results.appendLong(calculateGeohex(GEO.longAsPoint(encoded.getLong(firstValueIndex + i)), precision));
                }
                results.endPositionEntry();
            }
        }
    }

    protected static long calculateGeohex(Point point, GeoHexBoundedPredicate bounds) {
        // For points, filtering the point is as good as filtering the tile
        long geohex = H3.geoToH3(point.getLat(), point.getLon(), bounds.precision());
        if (bounds.validHex(geohex)) {
            return geohex;
        }
        // TODO: Are we sure negative numbers are not valid
        return -1L;
    }

    private static void fromWKB(LongBlock.Builder results, int position, BytesRefBlock wkbBlock, GeoHexBoundedPredicate bounds) {
        int valueCount = wkbBlock.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final BytesRef scratch = new BytesRef();
            final int firstValueIndex = wkbBlock.getFirstValueIndex(position);
            if (valueCount == 1) {
                long grid = calculateGeohex(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex, scratch)), bounds);
                if (grid < 0) {
                    results.appendNull();
                } else {
                    results.appendLong(grid);
                }
            } else {
                var gridIds = new ArrayList<Long>(valueCount);
                for (int i = 0; i < valueCount; i++) {
                    var grid = calculateGeohex(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex + i, scratch)), bounds);
                    if (grid >= 0) {
                        gridIds.add(grid);
                    }
                }
                addGrids(results, gridIds);
            }
        }
    }

    private static void fromEncodedLong(LongBlock.Builder results, int position, LongBlock encoded, GeoHexBoundedPredicate bounds) {
        int valueCount = encoded.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final int firstValueIndex = encoded.getFirstValueIndex(position);
            if (valueCount == 1) {
                long grid = calculateGeohex(GEO.longAsPoint(encoded.getLong(firstValueIndex)), bounds);
                if (grid < 0) {
                    results.appendNull();
                } else {
                    results.appendLong(grid);
                }
            } else {
                var gridIds = new ArrayList<Long>(valueCount);
                for (int i = 0; i < valueCount; i++) {
                    var grid = calculateGeohex(GEO.longAsPoint(encoded.getLong(firstValueIndex + i)), bounds);
                    if (grid >= 0) {
                        gridIds.add(grid);
                    }
                }
                addGrids(results, gridIds);
            }
        }
    }
}
