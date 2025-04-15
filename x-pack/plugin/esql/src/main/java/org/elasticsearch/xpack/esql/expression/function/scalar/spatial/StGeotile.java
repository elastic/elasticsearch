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
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileBoundedPredicate;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
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
 * Calculates the geotile of geo_point geometries.
 */
public class StGeotile extends SpatialGridFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StGeotile",
        StGeotile::new
    );

    @FunctionInfo(returnType = "long", description = """
        Calculates the `geotile` of the supplied geo_point at the specified precision.
        The result is long encoded. Use [ST_GEOTILE_TO_STRING](#esql-st_geotile_to_string) to convert the result to a string.
        Or use [ST_GEOTILE_TO_GEOSHAPE](#esql-st_geotile_to_geoshape) to convert either the long or string `geotile` to a
        POLYGON geo_shape.""", examples = @Example(file = "spatial-grid", tag = "st_geotile-grid"))
    public StGeotile(
        Source source,
        @Param(
            name = "geometry",
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

    private StGeotile(Source source, Expression field, Expression precision, Expression bounds, boolean spatialDocValues) {
        super(source, field, precision, bounds, spatialDocValues);
    }

    private StGeotile(StreamInput in) throws IOException {
        super(in, false);
    }

    @Override
    public SpatialGridFunction withDocValues(boolean useDocValues) {
        // Only update the docValues flags if the field is found in the attributes
        boolean docValues = this.spatialDocsValues || useDocValues;
        return new StGeotile(source(), spatialField, parameter, bounds, docValues);
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
        return new StGeotile(source(), newSpatialField, newParameter, newBounds);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeotile::new, spatialField, parameter, bounds);
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
                return new StGeotileFromLiteralAndFieldAndLiteralEvaluator.Factory(source(), point, toEvaluator.apply(parameter()), bbox);
            } else if (parameter().foldable()) {
                // Assume left is not foldable, since that would be dealt with in isFoldable() and fold()
                int precision = (int) parameter.fold(toEvaluator.foldCtx());
                GeoTileBoundedPredicate bounds = new GeoTileBoundedPredicate(precision, bbox);
                return spatialDocsValues
                    ? new StGeotileFromFieldDocValuesAndLiteralAndLiteralEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField()),
                        bounds
                    )
                    : new StGeotileFromFieldAndLiteralAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), bounds);
            } else {
                // Both arguments come from index fields
                return spatialDocsValues
                    ? new StGeotileFromFieldDocValuesAndFieldAndLiteralEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField),
                        toEvaluator.apply(parameter),
                        bbox
                    )
                    : new StGeotileFromFieldAndFieldAndLiteralEvaluator.Factory(
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
                return new StGeotileFromLiteralAndFieldEvaluator.Factory(source(), point, toEvaluator.apply(parameter()));
            } else if (parameter().foldable()) {
                // Assume left is not foldable, since that would be dealt with in isFoldable() and fold()
                int precision = (int) parameter.fold(toEvaluator.foldCtx());
                // TODO: Use GeoTileBoundedPredicate
                return spatialDocsValues
                    ? new StGeotileFromFieldDocValuesAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField()), precision)
                    : new StGeotileFromFieldAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), precision);
            } else {
                // Both arguments come from index fields
                return spatialDocsValues
                    ? new StGeotileFromFieldDocValuesAndFieldEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField),
                        toEvaluator.apply(parameter)
                    )
                    : new StGeotileFromFieldAndFieldEvaluator.Factory(
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
        // TODO: Use GeoTileBoundedPredicate
        return calculateGeotile(GEO.wkbAsPoint(point), precision);
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
        return calculateGeotile(GEO.wkbAsPoint(in), precision);
    }

    protected static long calculateGeotile(Point point, int precision) {
        return GeoTileUtils.longEncode(point.getX(), point.getY(), precision);
    }

    @Evaluator(extraName = "FromFieldAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndLiteralAndLiteral(LongBlock.Builder results, int p, BytesRefBlock in, @Fixed GeoTileBoundedPredicate bounds) {
        fromWKB(results, p, in, bounds);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteralAndLiteral(
        LongBlock.Builder results,
        int p,
        LongBlock encoded,
        @Fixed GeoTileBoundedPredicate bounds
    ) {
        fromEncodedLong(results, p, encoded, bounds);
    }

    @Evaluator(extraName = "FromFieldAndFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndFieldAndLiteral(LongBlock.Builder results, int p, BytesRefBlock in, int precision, @Fixed GeoBoundingBox bbox) {
        GeoTileBoundedPredicate bounds = new GeoTileBoundedPredicate(precision, bbox);
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
        GeoTileBoundedPredicate bounds = new GeoTileBoundedPredicate(precision, bbox);
        fromEncodedLong(results, p, encoded, bounds);
    }

    @Evaluator(extraName = "FromLiteralAndFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static long fromLiteralAndFieldAndLiteral(@Fixed BytesRef in, int precision, @Fixed GeoBoundingBox bbox) {
        GeoTileBoundedPredicate bounds = new GeoTileBoundedPredicate(precision, bbox);
        return calculateGeotile(GEO.wkbAsPoint(in), bounds);
    }

    private static void fromWKB(LongBlock.Builder results, int position, BytesRefBlock wkbBlock, int precision) {
        int valueCount = wkbBlock.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final BytesRef scratch = new BytesRef();
            final int firstValueIndex = wkbBlock.getFirstValueIndex(position);
            if (valueCount == 1) {
                results.appendLong(calculateGeotile(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex, scratch)), precision));
            } else {
                results.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    results.appendLong(calculateGeotile(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex + i, scratch)), precision));
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
                results.appendLong(calculateGeotile(GEO.longAsPoint(encoded.getLong(firstValueIndex)), precision));
            } else {
                results.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    results.appendLong(calculateGeotile(GEO.longAsPoint(encoded.getLong(firstValueIndex + i)), precision));
                }
                results.endPositionEntry();
            }
        }
    }

    protected static long calculateGeotile(Point point, GeoTileBoundedPredicate bounds) {
        final int tiles = 1 << GeoTileUtils.checkPrecisionRange(bounds.precision());
        final int x = GeoTileUtils.getXTile(point.getX(), tiles);
        final int y = GeoTileUtils.getYTile(point.getY(), tiles);
        if (bounds.validTile(x, y, bounds.precision())) {
            return GeoTileUtils.longEncodeTiles(bounds.precision(), x, y);
        }
        // TODO: Are we sure negative numbers are not valid
        return -1L;
    }

    private static void fromWKB(LongBlock.Builder results, int position, BytesRefBlock wkbBlock, GeoTileBoundedPredicate bounds) {
        int valueCount = wkbBlock.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final BytesRef scratch = new BytesRef();
            final int firstValueIndex = wkbBlock.getFirstValueIndex(position);
            if (valueCount == 1) {
                long grid = calculateGeotile(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex, scratch)), bounds);
                if (grid < 0) {
                    results.appendNull();
                } else {
                    results.appendLong(grid);
                }
            } else {
                var gridIds = new ArrayList<Long>(valueCount);
                for (int i = 0; i < valueCount; i++) {
                    var grid = calculateGeotile(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex + i, scratch)), bounds);
                    if (grid >= 0) {
                        gridIds.add(grid);
                    }
                }
                addGrids(results, gridIds);
            }
        }
    }

    private static void fromEncodedLong(LongBlock.Builder results, int position, LongBlock encoded, GeoTileBoundedPredicate bounds) {
        int valueCount = encoded.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final int firstValueIndex = encoded.getFirstValueIndex(position);
            if (valueCount == 1) {
                long grid = calculateGeotile(GEO.longAsPoint(encoded.getLong(firstValueIndex)), bounds);
                if (grid < 0) {
                    results.appendNull();
                } else {
                    results.appendLong(grid);
                }
            } else {
                var gridIds = new ArrayList<Long>(valueCount);
                for (int i = 0; i < valueCount; i++) {
                    var grid = calculateGeotile(GEO.longAsPoint(encoded.getLong(firstValueIndex + i)), bounds);
                    if (grid >= 0) {
                        gridIds.add(grid);
                    }
                }
                addGrids(results, gridIds);
            }
        }
    }
}
