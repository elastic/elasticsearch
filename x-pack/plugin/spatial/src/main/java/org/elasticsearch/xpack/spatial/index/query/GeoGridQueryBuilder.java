/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.h3.H3;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoPointScriptFieldType;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;
import org.elasticsearch.xpack.spatial.common.H3SphericalUtil;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;

import java.io.IOException;
import java.util.Objects;

/**
 * Creates a Lucene query that will filter for all documents that intersects the specified
 * bin of a grid.
 *
 * It supports geohash and geotile grids for both GeoShape and GeoPoint and the geohex grid
 * only for GeoPoint.
 * */
public class GeoGridQueryBuilder extends AbstractQueryBuilder<GeoGridQueryBuilder> {
    public static final String NAME = "geo_grid";

    /** Grids supported by this query */
    public enum Grid {
        GEOHASH {

            private static final String name = "geohash";

            @Override
            protected Query toQuery(SearchExecutionContext context, String fieldName, MappedFieldType fieldType, String id) {
                if (fieldType instanceof GeoShapeQueryable geoShapeQueryable) {
                    return geoShapeQueryable.geoShapeQuery(context, fieldName, ShapeRelation.INTERSECTS, getQueryHash(id));
                }
                throw new QueryShardException(
                    context,
                    "Field [" + fieldName + "] is of unsupported type [" + fieldType.typeName() + "] for [" + NAME + "] query"
                );
            }

            @Override
            protected String getName() {
                return name;
            }

            @Override
            protected void validate(String gridId) {
                Geohash.mortonEncode(gridId);
            }
        },
        GEOTILE {

            private static final String name = "geotile";

            @Override
            protected Query toQuery(SearchExecutionContext context, String fieldName, MappedFieldType fieldType, String id) {
                if (fieldType instanceof GeoShapeQueryable geoShapeQueryable) {
                    return geoShapeQueryable.geoShapeQuery(context, fieldName, ShapeRelation.INTERSECTS, getQueryTile(id));
                }
                throw new QueryShardException(
                    context,
                    "Field [" + fieldName + "] is of unsupported type [" + fieldType.typeName() + "] for [" + NAME + "] query"
                );
            }

            @Override
            protected String getName() {
                return name;
            }

            @Override
            protected void validate(String gridId) {
                GeoTileUtils.longEncode(gridId);
            }
        },

        GEOHEX {
            private static final String name = "geohex";

            @Override
            protected Query toQuery(SearchExecutionContext context, String fieldName, MappedFieldType fieldType, String id) {
                final long h3 = H3.stringToH3(id);
                if (fieldType instanceof GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType geoShapeFieldType) {
                    // shapes are solved on the cartesian geometry
                    final LatLonGeometry geometry = H3CartesianUtil.getLatLonGeometry(h3);
                    return geoShapeFieldType.geoShapeQuery(context, fieldName, ShapeRelation.INTERSECTS, geometry);
                } else {
                    // points are solved on the spherical geometry
                    final LatLonGeometry geometry = H3SphericalUtil.getLatLonGeometry(h3);
                    if (fieldType instanceof GeoPointFieldMapper.GeoPointFieldType pointFieldType) {
                        return pointFieldType.geoShapeQuery(context, fieldName, ShapeRelation.INTERSECTS, geometry);
                    } else if (fieldType instanceof GeoPointScriptFieldType scriptType) {
                        return scriptType.geoShapeQuery(context, fieldName, ShapeRelation.INTERSECTS, geometry);
                    }
                }
                throw new QueryShardException(
                    context,
                    "Field [" + fieldName + "] is of unsupported type [" + fieldType.typeName() + "] for [" + NAME + "] query"
                );
            }

            @Override
            protected String getName() {
                return name;
            }

            @Override
            protected void validate(String gridId) {
                boolean valid;
                try {
                    valid = H3.h3IsValid(gridId);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Invalid h3 address [" + gridId + "]", e);
                }
                if (valid == false) {
                    throw new IllegalArgumentException("Invalid h3 address [" + gridId + "]");
                }
            }
        };

        protected abstract Query toQuery(SearchExecutionContext context, String fieldName, MappedFieldType fieldType, String id);

        protected abstract String getName();

        protected abstract void validate(String gridId);

        private static Grid fromName(String name) {
            if (GEOHEX.getName().equals(name)) {
                return GEOHEX;
            } else if (GEOTILE.getName().equals(name)) {
                return GEOTILE;
            } else if (GEOHASH.getName().equals(name)) {
                return GEOHASH;
            } else {
                throw new ElasticsearchParseException("failed to parse [{}] query. Invalid grid name [" + name + "]", NAME);
            }
        }

    }

    // public for testing
    public static Rectangle getQueryTile(String id) {
        final Rectangle rectangle = GeoTileUtils.toBoundingBox(id);
        final int minY = GeoEncodingUtils.encodeLatitude(rectangle.getMinLat());
        final int minX = GeoEncodingUtils.encodeLongitude(rectangle.getMinLon());
        final int maxY = GeoEncodingUtils.encodeLatitude(rectangle.getMaxLat());
        final int maxX = GeoEncodingUtils.encodeLongitude(rectangle.getMaxLon());
        return new Rectangle(
            GeoEncodingUtils.decodeLongitude(minX),
            GeoEncodingUtils.decodeLongitude(maxX == Integer.MAX_VALUE ? maxX : maxX - 1),
            GeoEncodingUtils.decodeLatitude(maxY),
            GeoEncodingUtils.decodeLatitude(minY == GeoTileUtils.ENCODED_NEGATIVE_LATITUDE_MASK ? minY : minY + 1)
        );
    }

    // public for testing
    public static Rectangle getQueryHash(String id) {
        final Rectangle rectangle = Geohash.toBoundingBox(id);
        final int minX = GeoEncodingUtils.encodeLongitude(rectangle.getMinLon());
        final int minY = GeoEncodingUtils.encodeLatitude(rectangle.getMinLat());
        final int maxX = GeoEncodingUtils.encodeLongitude(rectangle.getMaxLon());
        final int maxY = GeoEncodingUtils.encodeLatitude(rectangle.getMaxLat());
        return new Rectangle(
            GeoEncodingUtils.decodeLongitude(minX),
            GeoEncodingUtils.decodeLongitude(maxX == Integer.MAX_VALUE ? maxX : maxX - 1),
            GeoEncodingUtils.decodeLatitude(maxY == Integer.MAX_VALUE ? maxY : maxY - 1),
            GeoEncodingUtils.decodeLatitude(minY)
        );
    }

    /**
     * The default value for ignore_unmapped.
     */
    private static final boolean DEFAULT_IGNORE_UNMAPPED = false;
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    /** Name of field holding geo coordinates to compute the bounding box on.*/
    private final String fieldName;
    private Grid grid;
    private String gridId;
    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    /**
     * Create new grid query.
     * @param fieldName name of index field containing geo coordinates to operate on.
     * */
    public GeoGridQueryBuilder(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("Field name must not be empty.");
        }
        this.fieldName = fieldName;
    }

    /**
     * Read from a stream.
     */
    public GeoGridQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        grid = Grid.fromName(in.readString());
        gridId = in.readString();
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(grid.getName());
        out.writeString(gridId);
        out.writeBoolean(ignoreUnmapped);
    }

    /**
     * Adds the grid and the gridId
     * @param grid The type of grid
     * @param gridId The grid bin identifier
     */
    public GeoGridQueryBuilder setGridId(Grid grid, String gridId) {
        grid.validate(gridId);
        this.grid = grid;
        this.gridId = gridId;
        return this;
    }

    /** Returns the name of the field to base the grid computation on. */
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * Sets whether the query builder should ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public GeoGridQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    public Query doToQuery(SearchExecutionContext context) {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "failed to find geo field [" + fieldName + "]");
            }
        }
        return new ConstantScoreQuery(grid.toQuery(context, fieldName, fieldType, gridId));
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.startObject(fieldName);
        builder.field(grid.getName(), gridId);
        builder.endObject();
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);

        boostAndQueryNameToXContent(builder);

        builder.endObject();
    }

    public static GeoGridQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;
        Grid grid = null;
        String gridId = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        if (grid != null) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "failed to parse [{}] query. unexpected field [{}]",
                                NAME,
                                parser.currentName()
                            );
                        }
                        grid = Grid.fromName(parser.currentName());
                        if (parser.nextToken().isValue()) {
                            gridId = parser.text();
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "failed to parse [{}] query. unexpected field [{}]",
                                NAME,
                                parser.currentName()
                            );
                        }
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "failed to parse [{}] query. unexpected field [{}]",
                            NAME,
                            currentFieldName
                        );
                    }
                }
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "failed to parse [{}] query. unexpected field [{}]",
                        NAME,
                        currentFieldName
                    );
                }
            }
        }

        if (grid == null) {
            throw new ElasticsearchParseException("failed to parse [{}] query. grid name not provided", NAME);
        }
        if (gridId == null) {
            throw new ElasticsearchParseException("failed to parse [{}] query. grid id not provided", NAME);
        }
        GeoGridQueryBuilder builder = new GeoGridQueryBuilder(fieldName);
        builder.setGridId(grid, gridId);
        builder.queryName(queryName);
        builder.boost(boost);
        builder.ignoreUnmapped(ignoreUnmapped);
        return builder;
    }

    @Override
    protected boolean doEquals(GeoGridQueryBuilder other) {
        return Objects.equals(grid, other.grid)
            && Objects.equals(gridId, other.gridId)
            && Objects.equals(fieldName, other.fieldName)
            && Objects.equals(ignoreUnmapped, other.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(grid, gridId, fieldName, ignoreUnmapped);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_3_0;
    }
}
