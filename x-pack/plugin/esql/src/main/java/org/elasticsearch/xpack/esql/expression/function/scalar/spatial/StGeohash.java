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

    /**
     * When checking grid cells with bounds, we need to check if the cell is valid (intersects with the bounds).
     * This uses GeoHashBoundedPredicate to check if the grid cell is valid.
     */
    protected static class GeoHashBoundedGrid implements BoundedGrid {
        private final int precision;
        private final GeoBoundingBox bbox;
        private final GeoHashBoundedPredicate bounds;

        public GeoHashBoundedGrid(int precision, GeoBoundingBox bbox) {
            this.precision = checkPrecisionRange(precision);
            this.bbox = bbox;
            this.bounds = new GeoHashBoundedPredicate(precision, bbox);
        }

        public long calculateGridId(Point point) {
            String geohash = Geohash.stringEncode(point.getX(), point.getY(), precision);
            if (bounds.validHash(geohash)) {
                return Geohash.longEncode(geohash);
            }
            // TODO: Are negative values allowed in geohash long encoding?
            return -1;
        }

        @Override
        public int precision() {
            return precision;
        }

        @Override
        public String toString() {
            return "[" + bbox + "]";
        }
    }

    /**
     * For unbounded grids, we don't need to check if the grid cell is valid,
     * just calculate the encoded long intersecting the point at that precision.
     */
    protected static final UnboundedGrid unboundedGrid = (point, precision) -> Geohash.longEncode(
        point.getX(),
        point.getY(),
        checkPrecisionRange(precision)
    );

    private static int checkPrecisionRange(int precision) {
        if (precision < 1 || precision > Geohash.PRECISION) {
            throw new IllegalArgumentException(
                "Invalid geohash_grid precision of " + precision + ". Must be between 1 and " + Geohash.PRECISION + "."
            );
        }
        return precision;
    }

    @FunctionInfo(
        returnType = "long",
        description = """
            Calculates the `geohash` of the supplied geo_point at the specified precision.
            The result is long encoded. Use [ST_GEOHASH_TO_STRING](#esql-st_geohash_to_string) to convert the result to a string.

            These functions are related to the [`geo_grid` query](/reference/query-languages/query-dsl/query-dsl-geo-grid-query.md)
            and the [`geohash_grid` aggregation](/reference/aggregations/search-aggregations-bucket-geohashgrid-aggregation.md).""",
        examples = @Example(file = "spatial-grid", tag = "st_geohash-grid")
    )
    public StGeohash(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point" },
            description = "Expression of type `geo_point`. If `null`, the function returns `null`."
        ) Expression field,
        @Param(name = "precision", type = { "integer" }, description = """
            Expression of type `integer`. If `null`, the function returns `null`.
            Valid values are between [1 and 12](https://en.wikipedia.org/wiki/Geohash).""") Expression precision,
        @Param(name = "bounds", type = { "geo_shape" }, description = """
            Optional bounds to filter the grid tiles, a `geo_shape`.
            The envelope of the `geo_shape` is used as bounds.""", optional = true) Expression bounds
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
        if (parameter().foldable() == false) {
            throw new IllegalArgumentException("precision must be foldable");
        }
        if (bounds != null) {
            if (bounds.foldable() == false) {
                throw new IllegalArgumentException("bounds must be foldable");
            }
            GeoBoundingBox bbox = asGeoBoundingBox(bounds.fold(toEvaluator.foldCtx()));
            int precision = (int) parameter.fold(toEvaluator.foldCtx());
            GeoHashBoundedGrid bounds = new GeoHashBoundedGrid(precision, bbox);
            return spatialDocsValues
                ? new StGeohashFromFieldDocValuesAndLiteralAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField()), bounds)
                : new StGeohashFromFieldAndLiteralAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), bounds);
        } else {
            int precision = checkPrecisionRange((int) parameter.fold(toEvaluator.foldCtx()));
            return spatialDocsValues
                ? new StGeohashFromFieldDocValuesAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField()), precision)
                : new StGeohashFromFieldAndLiteralEvaluator.Factory(source(), toEvaluator.apply(spatialField), precision);
        }
    }

    @Override
    public Object fold(FoldContext ctx) {
        var point = (BytesRef) spatialField().fold(ctx);
        int precision = (int) parameter().fold(ctx);
        if (bounds() == null) {
            return unboundedGrid.calculateGridId(GEO.wkbAsPoint(point), precision);
        } else {
            GeoBoundingBox bbox = asGeoBoundingBox(bounds().fold(ctx));
            GeoHashBoundedGrid bounds = new GeoHashBoundedGrid(precision, bbox);
            return bounds.calculateGridId(GEO.wkbAsPoint(point));
        }
    }

    @Evaluator(extraName = "FromFieldAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndLiteral(LongBlock.Builder results, int p, BytesRefBlock wkbBlock, @Fixed int precision) {
        fromWKB(results, p, wkbBlock, precision, unboundedGrid);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteral(LongBlock.Builder results, int p, LongBlock encoded, @Fixed int precision) {
        fromEncodedLong(results, p, encoded, precision, unboundedGrid);
    }

    @Evaluator(extraName = "FromFieldAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldAndLiteralAndLiteral(LongBlock.Builder results, int p, BytesRefBlock in, @Fixed GeoHashBoundedGrid bounds) {
        fromWKB(results, p, in, bounds);
    }

    @Evaluator(extraName = "FromFieldDocValuesAndLiteralAndLiteral", warnExceptions = { IllegalArgumentException.class })
    static void fromFieldDocValuesAndLiteralAndLiteral(
        LongBlock.Builder results,
        int p,
        LongBlock encoded,
        @Fixed GeoHashBoundedGrid bounds
    ) {
        fromEncodedLong(results, p, encoded, bounds);
    }
}
