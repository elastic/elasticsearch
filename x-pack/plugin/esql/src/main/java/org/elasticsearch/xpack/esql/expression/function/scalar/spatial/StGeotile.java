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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
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

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
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

    @FunctionInfo(
        returnType = "keyword",
        description = "Calculates the `geotile` of the supplied geo_point at the specified precision.",
        examples = @Example(file = "spatial-grid", tag = "st_geotile-grid")
    )
    public StGeotile(
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
            description = "Bounds to filter the grid tiles, either a geo_shape BBOX or an array",
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
        return KEYWORD;
    }

    @Override
    protected SpatialGridFunction replaceChildren(Expression newSpatialField, Expression newParameter, Expression newBounds) {
        return new StGeotile(source(), newSpatialField, newParameter, newBounds);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeotile::new, spatialField, parameter, bounds, false);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (bounds != null) {
            if (bounds.foldable() == false) {
                throw new IllegalArgumentException("bounds must foldable");
            }
            Rectangle bbox = asRectangle((BytesRef) bounds.fold(toEvaluator.foldCtx()));
            if (spatialField().foldable()) {
                // Assume right is not foldable, since that would be dealt with in isFoldable() and fold()
                var point = (BytesRef) spatialField.fold(toEvaluator.foldCtx());
                return new StGeotileFromLiteralAndFieldAndLiteralEvaluator.Factory(source(), point, toEvaluator.apply(parameter()), bbox);
            } else if (parameter().foldable()) {
                // Assume left is not foldable, since that would be dealt with in isFoldable() and fold()
                int precision = (int) parameter.fold(toEvaluator.foldCtx());
                return spatialDocsValues
                    ? new StGeotileFromFieldDocValuesAndLiteralAndLiteralEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField()),
                        precision,
                        bbox
                    )
                    : new StGeotileFromFieldAndLiteralAndLiteralEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField),
                        precision,
                        bbox
                    );
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
        return calculateGeotile(GEO.wkbAsPoint(point), precision);
    }

    @Evaluator(extraName = "FromFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndLiteral(BytesRefBlock.Builder results, int p, BytesRefBlock wkbBlock, @Fixed int precision) {
        fromWKB(results, p, wkbBlock, precision);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteral(BytesRefBlock.Builder results, int p, LongBlock encoded, @Fixed int precision) {
        fromEncodedLong(results, p, encoded, precision);
    }

    @Evaluator(extraName = "FromFieldAndField", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndField(BytesRefBlock.Builder results, int p, BytesRefBlock in, int precision) {
        fromWKB(results, p, in, precision);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndField", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndField(BytesRefBlock.Builder results, int p, LongBlock encoded, int precision) {
        fromEncodedLong(results, p, encoded, precision);
    }

    @Evaluator(extraName = "FromLiteralAndField", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromLiteralAndField(@Fixed BytesRef in, int precision) {
        return calculateGeotile(GEO.wkbAsPoint(in), precision);
    }

    protected static BytesRef calculateGeotile(Point point, int precision) {
        return new BytesRef(GeoTileUtils.stringEncode(GeoTileUtils.longEncode(point.getX(), point.getY(), precision)));
    }

    @Evaluator(extraName = "FromFieldAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndLiteralAndLiteral(
        BytesRefBlock.Builder results,
        int p,
        BytesRefBlock in,
        @Fixed int precision,
        @Fixed Rectangle bounds
    ) {
        fromWKB(results, p, in, precision, bounds);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteralAndLiteral(
        BytesRefBlock.Builder results,
        int p,
        LongBlock encoded,
        @Fixed int precision,
        @Fixed Rectangle bounds
    ) {
        fromEncodedLong(results, p, encoded, precision, bounds);
    }

    @Evaluator(extraName = "FromFieldAndFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndFieldAndLiteral(
        BytesRefBlock.Builder results,
        int p,
        BytesRefBlock in,
        int precision,
        @Fixed Rectangle bounds
    ) {
        fromWKB(results, p, in, precision, bounds);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndFieldAndLiteral(
        BytesRefBlock.Builder results,
        int p,
        LongBlock encoded,
        int precision,
        @Fixed Rectangle bounds
    ) {
        fromEncodedLong(results, p, encoded, precision, bounds);
    }

    @Evaluator(extraName = "FromLiteralAndFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromLiteralAndFieldAndLiteral(@Fixed BytesRef in, int precision, @Fixed Rectangle bounds) {
        return calculateGeotile(GEO.wkbAsPoint(in), precision, bounds);
    }

    private static void fromWKB(BytesRefBlock.Builder results, int position, BytesRefBlock wkbBlock, int precision) {
        int valueCount = wkbBlock.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final BytesRef scratch = new BytesRef();
            final int firstValueIndex = wkbBlock.getFirstValueIndex(position);
            if (valueCount == 1) {
                results.appendBytesRef(calculateGeotile(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex, scratch)), precision));
            } else {
                results.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    results.appendBytesRef(calculateGeotile(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex + i, scratch)), precision));
                }
                results.endPositionEntry();
            }
        }
    }

    private static void fromEncodedLong(BytesRefBlock.Builder results, int position, LongBlock encoded, int precision) {
        int valueCount = encoded.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final int firstValueIndex = encoded.getFirstValueIndex(position);
            if (valueCount == 1) {
                results.appendBytesRef(calculateGeotile(GEO.longAsPoint(encoded.getLong(firstValueIndex)), precision));
            } else {
                results.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    results.appendBytesRef(calculateGeotile(GEO.longAsPoint(encoded.getLong(firstValueIndex + i)), precision));
                }
                results.endPositionEntry();
            }
        }
    }

    protected static BytesRef calculateGeotile(Point point, int precision, Rectangle bounds) {
        // For points, filtering the point is as good as filtering the tile
        if (inBounds(point, bounds)) {
            return new BytesRef(GeoTileUtils.stringEncode(GeoTileUtils.longEncode(point.getX(), point.getY(), precision)));
        }
        return null;
    }

    private static void fromWKB(BytesRefBlock.Builder results, int position, BytesRefBlock wkbBlock, int precision, Rectangle bounds) {
        int valueCount = wkbBlock.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final BytesRef scratch = new BytesRef();
            final int firstValueIndex = wkbBlock.getFirstValueIndex(position);
            if (valueCount == 1) {
                BytesRef grid = calculateGeotile(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex, scratch)), precision, bounds);
                if (grid == null) {
                    results.appendNull();
                } else {
                    results.appendBytesRef(grid);
                }
            } else {
                var gridIds = new ArrayList<BytesRef>(valueCount);
                for (int i = 0; i < valueCount; i++) {
                    var grid = calculateGeotile(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex + i, scratch)), precision, bounds);
                    if (grid != null) {
                        gridIds.add(grid);
                    }
                }
                addGrids(results, gridIds);
            }
        }
    }

    private static void fromEncodedLong(BytesRefBlock.Builder results, int position, LongBlock encoded, int precision, Rectangle bounds) {
        int valueCount = encoded.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final int firstValueIndex = encoded.getFirstValueIndex(position);
            if (valueCount == 1) {
                BytesRef grid = calculateGeotile(GEO.longAsPoint(encoded.getLong(firstValueIndex)), precision, bounds);
                if (grid == null) {
                    results.appendNull();
                } else {
                    results.appendBytesRef(grid);
                }
            } else {
                var gridIds = new ArrayList<BytesRef>(valueCount);
                for (int i = 0; i < valueCount; i++) {
                    var grid = calculateGeotile(GEO.longAsPoint(encoded.getLong(firstValueIndex + i)), precision, bounds);
                    if (grid != null) {
                        gridIds.add(grid);
                    }
                }
                addGrids(results, gridIds);
            }
        }
    }
}
