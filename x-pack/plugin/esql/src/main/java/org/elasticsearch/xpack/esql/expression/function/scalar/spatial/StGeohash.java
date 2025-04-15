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
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashBoundedPredicate;
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
 * Calculates the geohash of geo_point geometries.
 */
public class StGeohash extends SpatialGridFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StGeohash",
        StGeohash::new
    );

    @FunctionInfo(returnType = "long", description = """
        Calculates the `geohash` of the supplied geo_point at the specified precision.
        The result is long encoded. Use [ST_GEOHASH_TO_STRING](#esql-st_geohash_to_string) to convert the result to a string.
        Or use [ST_GEOHASH_TO_GEOSHAPE](#esql-st_geohash_to_geoshape) to convert either the long or string `geohash` to a
        POLYGON geo_shape.""", examples = @Example(file = "spatial-grid", tag = "st_geohash-grid"))
    public StGeohash(
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

    private StGeohash(Source source, Expression field, Expression precision, Expression bounds, boolean spatialDocValues) {
        super(source, field, precision, bounds, spatialDocValues);
    }

    private StGeohash(StreamInput in) throws IOException {
        super(in, false);
    }

    @Override
    public SpatialGridFunction withDocValues(boolean useDocValues) {
        // Only update the docValues flags if the field is found in the attributes
        boolean docValues = this.spatialDocsValues || useDocValues;
        return new StGeohash(source(), spatialField, parameter, bounds, docValues);
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
        return new StGeohash(source(), newSpatialField, newParameter, newBounds);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeohash::new, spatialField, parameter, bounds);
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
                return new StGeohashFromLiteralAndFieldAndLiteralEvaluator.Factory(source(), point, toEvaluator.apply(parameter()), bbox);
            } else if (parameter().foldable()) {
                // Assume left is not foldable, since that would be dealt with in isFoldable() and fold()
                int precision = (int) parameter.fold(toEvaluator.foldCtx());
                GeoHashBoundedPredicate bounds = new GeoHashBoundedPredicate(precision, bbox);
                return spatialDocsValues
                    ? new StGeohashFromFieldDocValuesAndLiteralAndLiteralEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField()),
                        bounds
                    )
                    : new StGeohashFromFieldAndLiteralAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), bounds);
            } else {
                // Both arguments come from index fields
                return spatialDocsValues
                    ? new StGeohashFromFieldDocValuesAndFieldAndLiteralEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField),
                        toEvaluator.apply(parameter),
                        bbox
                    )
                    : new StGeohashFromFieldAndFieldAndLiteralEvaluator.Factory(
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
                return new StGeohashFromLiteralAndFieldEvaluator.Factory(source(), point, toEvaluator.apply(parameter()));
            } else if (parameter().foldable()) {
                // Assume left is not foldable, since that would be dealt with in isFoldable() and fold()
                int precision = (int) parameter.fold(toEvaluator.foldCtx());
                return spatialDocsValues
                    ? new StGeohashFromFieldDocValuesAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField()), precision)
                    : new StGeohashFromFieldAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), precision);
            } else {
                // Both arguments come from index fields
                return spatialDocsValues
                    ? new StGeohashFromFieldDocValuesAndFieldEvaluator.Factory(
                        source(),
                        toEvaluator.apply(spatialField),
                        toEvaluator.apply(parameter)
                    )
                    : new StGeohashFromFieldAndFieldEvaluator.Factory(
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
        // TODO: Use GeoHashBoundedPredicate
        return calculateGeohash(GEO.wkbAsPoint(point), precision);
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
        return calculateGeohash(GEO.wkbAsPoint(in), precision);
    }

    protected static long calculateGeohash(Point point, int precision) {
        return Geohash.longEncode(point.getX(), point.getY(), precision);
    }

    @Evaluator(extraName = "FromFieldAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndLiteralAndLiteral(LongBlock.Builder results, int p, BytesRefBlock in, @Fixed GeoHashBoundedPredicate bounds) {
        fromWKB(results, p, in, bounds);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteralAndLiteral(
        LongBlock.Builder results,
        int p,
        LongBlock encoded,
        @Fixed GeoHashBoundedPredicate bounds
    ) {
        fromEncodedLong(results, p, encoded, bounds);
    }

    @Evaluator(extraName = "FromFieldAndFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndFieldAndLiteral(LongBlock.Builder results, int p, BytesRefBlock in, int precision, @Fixed GeoBoundingBox bbox) {
        GeoHashBoundedPredicate bounds = new GeoHashBoundedPredicate(precision, bbox);
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
        GeoHashBoundedPredicate bounds = new GeoHashBoundedPredicate(precision, bbox);
        fromEncodedLong(results, p, encoded, bounds);
    }

    @Evaluator(extraName = "FromLiteralAndFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static long fromLiteralAndFieldAndLiteral(@Fixed BytesRef in, int precision, @Fixed GeoBoundingBox bbox) {
        GeoHashBoundedPredicate bounds = new GeoHashBoundedPredicate(precision, bbox);
        return calculateGeohash(GEO.wkbAsPoint(in), bounds);
    }

    private static void fromWKB(LongBlock.Builder results, int position, BytesRefBlock wkbBlock, int precision) {
        int valueCount = wkbBlock.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final BytesRef scratch = new BytesRef();
            final int firstValueIndex = wkbBlock.getFirstValueIndex(position);
            if (valueCount == 1) {
                results.appendLong(calculateGeohash(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex, scratch)), precision));
            } else {
                results.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    results.appendLong(calculateGeohash(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex + i, scratch)), precision));
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
                results.appendLong(calculateGeohash(GEO.longAsPoint(encoded.getLong(firstValueIndex)), precision));
            } else {
                results.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    results.appendLong(calculateGeohash(GEO.longAsPoint(encoded.getLong(firstValueIndex + i)), precision));
                }
                results.endPositionEntry();
            }
        }
    }

    protected static long calculateGeohash(Point point, GeoHashBoundedPredicate bounds) {
        String geohash = Geohash.stringEncode(point.getX(), point.getY(), bounds.precision());
        if (bounds.validHash(geohash)) {
            return Geohash.longEncode(geohash);
        }
        // TODO: Are negative values allowed in geohash long encoding?
        return -1;
    }

    private static void fromWKB(LongBlock.Builder results, int position, BytesRefBlock wkbBlock, GeoHashBoundedPredicate bounds) {
        int valueCount = wkbBlock.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final BytesRef scratch = new BytesRef();
            final int firstValueIndex = wkbBlock.getFirstValueIndex(position);
            if (valueCount == 1) {
                long grid = calculateGeohash(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex, scratch)), bounds);
                if (grid < 0) {
                    results.appendNull();
                } else {
                    results.appendLong(grid);
                }
            } else {
                var gridIds = new ArrayList<Long>(valueCount);
                for (int i = 0; i < valueCount; i++) {
                    var grid = calculateGeohash(GEO.wkbAsPoint(wkbBlock.getBytesRef(firstValueIndex + i, scratch)), bounds);
                    if (grid >= 0) {
                        gridIds.add(grid);
                    }
                }
                addGrids(results, gridIds);
            }
        }
    }

    private static void fromEncodedLong(LongBlock.Builder results, int position, LongBlock encoded, GeoHashBoundedPredicate bounds) {
        int valueCount = encoded.getValueCount(position);
        if (valueCount < 1) {
            results.appendNull();
        } else {
            final int firstValueIndex = encoded.getFirstValueIndex(position);
            if (valueCount == 1) {
                long grid = calculateGeohash(GEO.longAsPoint(encoded.getLong(firstValueIndex)), bounds);
                if (grid < 0) {
                    results.appendNull();
                } else {
                    results.appendLong(grid);
                }
            } else {
                var gridIds = new ArrayList<Long>(valueCount);
                for (int i = 0; i < valueCount; i++) {
                    var grid = calculateGeohash(GEO.longAsPoint(encoded.getLong(firstValueIndex + i)), bounds);
                    if (grid >= 0) {
                        gridIds.add(grid);
                    }
                }
                addGrids(results, gridIds);
            }
        }
    }
}
