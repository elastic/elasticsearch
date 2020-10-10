/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/*
 * A geotile_grid aggregation source for group_by
 */
public class GeoTileGroupSource extends SingleGroupSource {
    private static final String NAME = "transform_geo_tile_group";

    private static final ParseField PRECISION = new ParseField("precision");
    private static final ConstructingObjectParser<GeoTileGroupSource, Void> STRICT_PARSER = createParser(false);

    private static final ConstructingObjectParser<GeoTileGroupSource, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<GeoTileGroupSource, Void> createParser(boolean lenient) {
        ConstructingObjectParser<GeoTileGroupSource, Void> parser = new ConstructingObjectParser<>(NAME, lenient, (args) -> {
            String field = (String) args[0];
            boolean missingBucket = args[1] == null ? false : (boolean) args[1];
            Integer precision = (Integer) args[2];
            GeoBoundingBox boundingBox = (GeoBoundingBox) args[3];

            return new GeoTileGroupSource(field, missingBucket, precision, boundingBox);
        });
        parser.declareString(optionalConstructorArg(), FIELD);
        parser.declareBoolean(optionalConstructorArg(), MISSING_BUCKET);
        parser.declareInt(optionalConstructorArg(), PRECISION);
        parser.declareField(
            optionalConstructorArg(),
            (p, context) -> GeoBoundingBox.parseBoundingBox(p),
            GeoBoundingBox.BOUNDS_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        return parser;
    }

    private final Integer precision;
    private final GeoBoundingBox geoBoundingBox;

    public GeoTileGroupSource(final String field, final boolean missingBucket, final Integer precision, final GeoBoundingBox boundingBox) {
        super(field, null, missingBucket);
        if (precision != null) {
            GeoTileUtils.checkPrecisionRange(precision);
        }
        this.precision = precision;
        this.geoBoundingBox = boundingBox;
    }

    public GeoTileGroupSource(StreamInput in) throws IOException {
        super(in);
        precision = in.readOptionalVInt();
        geoBoundingBox = in.readOptionalWriteable(GeoBoundingBox::new);
    }

    @Override
    public Type getType() {
        return Type.GEOTILE_GRID;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalVInt(precision);
        out.writeOptionalWriteable(geoBoundingBox);
    }

    public Integer getPrecision() {
        return precision;
    }

    public GeoBoundingBox getGeoBoundingBox() {
        return geoBoundingBox;
    }

    public static GeoTileGroupSource fromXContent(final XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    @Override
    public boolean supportsIncrementalBucketUpdate() {
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        super.innerXContent(builder, params);
        if (precision != null) {
            builder.field(PRECISION.getPreferredName(), precision);
        }
        if (geoBoundingBox != null) {
            geoBoundingBox.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final GeoTileGroupSource that = (GeoTileGroupSource) other;

        return this.missingBucket == that.missingBucket
            && Objects.equals(this.field, that.field)
            && Objects.equals(this.precision, that.precision)
            && Objects.equals(this.geoBoundingBox, that.geoBoundingBox);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, missingBucket, precision, geoBoundingBox);
    }

    @Override
    public String getMappingType() {
        return GeoShapeFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Object transformBucketKey(Object key) {
        assert key instanceof String;
        Rectangle rectangle = GeoTileUtils.toBoundingBox(key.toString());
        final Map<String, Object> geoShape = new HashMap<>();
        geoShape.put(ShapeParser.FIELD_TYPE.getPreferredName(), PolygonBuilder.TYPE.shapeName());
        geoShape.put(
            ShapeParser.FIELD_COORDINATES.getPreferredName(),
            Collections.singletonList(
                Arrays.asList(
                    new Double[] { rectangle.getMaxLon(), rectangle.getMinLat() },
                    new Double[] { rectangle.getMinLon(), rectangle.getMinLat() },
                    new Double[] { rectangle.getMinLon(), rectangle.getMaxLat() },
                    new Double[] { rectangle.getMaxLon(), rectangle.getMaxLat() },
                    new Double[] { rectangle.getMaxLon(), rectangle.getMinLat() }
                )
            )
        );
        return geoShape;
    }

    private GeoBoundingBoxQueryBuilder toGeoQuery(Rectangle rectangle) {
        return QueryBuilders.geoBoundingBoxQuery(field)
            .setCorners(
                new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
            );
    }
}
