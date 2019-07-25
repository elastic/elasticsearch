/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.GeometryIndexer;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiLineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.MultiPolygonBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.GeometryCollection;
import org.elasticsearch.geo.geometry.GeometryVisitor;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiLine;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.MultiPolygon;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.geo.geometry.Rectangle;
import org.elasticsearch.index.mapper.BaseGeoShapeFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.GeoShapeFieldMapper.toLucenePolygon;

/**
 * Derived {@link AbstractGeometryQueryBuilder} that builds a lat, lon GeoShape Query
 */
public class GeoShapeQueryBuilder extends AbstractGeometryQueryBuilder<GeoShapeQueryBuilder> {
    public static final String NAME = "geo_shape";
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(GeoShapeQueryBuilder.class));

    protected static final ParseField STRATEGY_FIELD = new ParseField("strategy");

    private SpatialStrategy strategy;

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name using the given Shape
     *
     * @param fieldName
     *            Name of the field that will be queried
     * @param shape
     *            Shape used in the Query
     */
    public GeoShapeQueryBuilder(String fieldName, Geometry shape) {
        super(fieldName, shape);
    }

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name using the given Shape
     *
     * @param fieldName
     *            Name of the field that will be queried
     * @param shape
     *            Shape used in the Query
     *
     * @deprecated use {@link #GeoShapeQueryBuilder(String, Geometry)} instead
     */
    @Deprecated
    public GeoShapeQueryBuilder(String fieldName, ShapeBuilder shape) {
        super(fieldName, shape);
    }

    public GeoShapeQueryBuilder(String fieldName, Supplier<Geometry> shapeSupplier, String indexedShapeId,
                                @Nullable String indexedShapeType) {
        super(fieldName, shapeSupplier, indexedShapeId, indexedShapeType);
    }

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name and will use the Shape found with the given ID in the given
     * type
     *
     * @param fieldName
     *            Name of the field that will be filtered
     * @param indexedShapeId
     *            ID of the indexed Shape that will be used in the Query
     * @param indexedShapeType
     *            Index type of the indexed Shapes
     * @deprecated use {@link #GeoShapeQueryBuilder(String, String)} instead
     */
    @Deprecated
    public GeoShapeQueryBuilder(String fieldName, String indexedShapeId, String indexedShapeType) {
        super(fieldName, indexedShapeId, indexedShapeType);
    }

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name and will use the Shape found with the given ID
     *
     * @param fieldName
     *            Name of the field that will be filtered
     * @param indexedShapeId
     *            ID of the indexed Shape that will be used in the Query
     */
    public GeoShapeQueryBuilder(String fieldName, String indexedShapeId) {
        super(fieldName, indexedShapeId);
    }

    public GeoShapeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        strategy = in.readOptionalWriteable(SpatialStrategy::readFromStream);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeOptionalWriteable(strategy);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Sets the relation of query shape and indexed shape.
     *
     * @param relation relation of the shapes
     * @return this
     */
    public GeoShapeQueryBuilder relation(ShapeRelation relation) {
        if (relation == null) {
            throw new IllegalArgumentException("No Shape Relation defined");
        }
        if (SpatialStrategy.TERM.equals(strategy) && relation != ShapeRelation.INTERSECTS) {
            throw new IllegalArgumentException("current strategy [" + strategy.getStrategyName() + "] only supports relation ["
                + ShapeRelation.INTERSECTS.getRelationName() + "] found relation [" + relation.getRelationName() + "]");
        }
        this.relation = relation;
        return this;
    }

    /**
     * Defines which spatial strategy will be used for building the geo shape
     * Query. When not set, the strategy that will be used will be the one that
     * is associated with the geo shape field in the mappings.
     *
     * @param strategy
     *            The spatial strategy to use for building the geo shape Query
     * @return this
     */
    public GeoShapeQueryBuilder strategy(SpatialStrategy strategy) {
        if (strategy != null && strategy == SpatialStrategy.TERM && relation != ShapeRelation.INTERSECTS) {
            throw new IllegalArgumentException("strategy [" + strategy.getStrategyName() + "] only supports relation ["
                + ShapeRelation.INTERSECTS.getRelationName() + "] found relation [" + relation.getRelationName() + "]");
        }
        this.strategy = strategy;
        return this;
    }
    /**
     * @return The spatial strategy to use for building the geo shape Query
     */
    public SpatialStrategy strategy() {
        return strategy;
    }

    @Override
    protected List validContentTypes() {
        return Arrays.asList(BaseGeoShapeFieldMapper.CONTENT_TYPE);
    }

    @Override
    public String queryFieldType() {
        return BaseGeoShapeFieldMapper.CONTENT_TYPE;
    }

    @Override
    public void doShapeQueryXContent(XContentBuilder builder, Params params) throws IOException {
        if (strategy != null) {
            builder.field(STRATEGY_FIELD.getPreferredName(), strategy.getStrategyName());
        }
    }

    @Override
    protected GeoShapeQueryBuilder newShapeQueryBuilder(String fieldName, Geometry shape) {
        return new GeoShapeQueryBuilder(fieldName, shape);
    }

    @Override
    protected GeoShapeQueryBuilder newShapeQueryBuilder(String fieldName, Supplier<Geometry> shapeSupplier,
                                                        String indexedShapeId, String indexedShapeType) {
        return new GeoShapeQueryBuilder(fieldName, shapeSupplier, indexedShapeId, indexedShapeType);
    }

    @Override
    public Query buildShapeQuery(QueryShardContext context, MappedFieldType fieldType) {
        if (fieldType.typeName().equals(BaseGeoShapeFieldMapper.CONTENT_TYPE) == false) {
            throw new QueryShardException(context,
                "Field [" + fieldName + "] is not of type [" + queryFieldType() + "] but of type [" + fieldType.typeName() + "]");
        }

        final BaseGeoShapeFieldMapper.BaseGeoShapeFieldType ft = (BaseGeoShapeFieldMapper.BaseGeoShapeFieldType) fieldType;
        Query query;
        if (strategy != null || ft instanceof LegacyGeoShapeFieldMapper.GeoShapeFieldType) {
            LegacyGeoShapeFieldMapper.GeoShapeFieldType shapeFieldType = (LegacyGeoShapeFieldMapper.GeoShapeFieldType) ft;
            SpatialStrategy spatialStrategy = shapeFieldType.strategy();
            if (this.strategy != null) {
                spatialStrategy = this.strategy;
            }
            PrefixTreeStrategy prefixTreeStrategy = shapeFieldType.resolvePrefixTreeStrategy(spatialStrategy);
            if (prefixTreeStrategy instanceof RecursivePrefixTreeStrategy && relation == ShapeRelation.DISJOINT) {
                // this strategy doesn't support disjoint anymore: but it did
                // before, including creating lucene fieldcache (!)
                // in this case, execute disjoint as exists && !intersects
                BooleanQuery.Builder bool = new BooleanQuery.Builder();
                Query exists = ExistsQueryBuilder.newFilter(context, fieldName);
                Query intersects = prefixTreeStrategy.makeQuery(getArgs(shape, ShapeRelation.INTERSECTS));
                bool.add(exists, BooleanClause.Occur.MUST);
                bool.add(intersects, BooleanClause.Occur.MUST_NOT);
                query = new ConstantScoreQuery(bool.build());
            } else {
                query = new ConstantScoreQuery(prefixTreeStrategy.makeQuery(getArgs(shape, relation)));
            }
        } else {
            query = new ConstantScoreQuery(getVectorQuery(context, shape));
        }
        return query;
    }

    public static SpatialArgs getArgs(Geometry shape, ShapeRelation relation) {
        switch (relation) {
            case DISJOINT:
                return new SpatialArgs(SpatialOperation.IsDisjointTo, buildS4J(shape));
            case INTERSECTS:
                return new SpatialArgs(SpatialOperation.Intersects, buildS4J(shape));
            case WITHIN:
                return new SpatialArgs(SpatialOperation.IsWithin, buildS4J(shape));
            case CONTAINS:
                return new SpatialArgs(SpatialOperation.Contains, buildS4J(shape));
            default:
                throw new IllegalArgumentException("invalid relation [" + relation + "]");
        }
    }

    /**
     * Builds JTS shape from a geometry
     *
     * This method is needed to handle legacy indices and will be removed when we no longer need to build JTS shapes
     */
    private static Shape buildS4J(Geometry geometry) {
        return geometryToShapeBuilder(geometry).buildS4J();
    }

    private Query getVectorQuery(QueryShardContext context, Geometry queryShape) {
        // CONTAINS queries are not yet supported by VECTOR strategy
        if (relation == ShapeRelation.CONTAINS) {
            throw new QueryShardException(context,
                ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "]");
        }
        // wrap geoQuery as a ConstantScoreQuery
        return getVectorQueryFromShape(context, queryShape);
    }

    protected Query getVectorQueryFromShape(QueryShardContext context, Geometry queryShape) {
        // TODO: Move this to QueryShardContext
        GeometryIndexer geometryIndexer = new GeometryIndexer(true);

        Geometry processedShape = geometryIndexer.prepareForIndexing(queryShape);

        if (processedShape == null) {
            return new MatchNoDocsQuery();
        }
        return queryShape.visit(new ShapeVisitor(context));
    }

    public static ShapeBuilder<?, ?, ?> geometryToShapeBuilder(Geometry geometry) {
        ShapeBuilder<?, ?, ?> shapeBuilder = geometry.visit(new GeometryVisitor<>() {
            @Override
            public ShapeBuilder<?, ?, ?> visit(Circle circle) {
                throw new UnsupportedOperationException("circle is not supported");
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(GeometryCollection<?> collection) {
                GeometryCollectionBuilder shapes = new GeometryCollectionBuilder();
                for (Geometry geometry : collection) {
                    shapes.shape(geometry.visit(this));
                }
                return shapes;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(org.elasticsearch.geo.geometry.Line line) {
                List<Coordinate> coordinates = new ArrayList<>();
                for (int i = 0; i < line.length(); i++) {
                    coordinates.add(new Coordinate(line.getLon(i), line.getLat(i), line.getAlt(i)));
                }
                return new LineStringBuilder(coordinates);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(LinearRing ring) {
                throw new UnsupportedOperationException("circle is not supported");
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiLine multiLine) {
                MultiLineStringBuilder lines = new MultiLineStringBuilder();
                for (int i = 0; i < multiLine.size(); i++) {
                    lines.linestring((LineStringBuilder) visit(multiLine.get(i)));
                }
                return lines;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiPoint multiPoint) {
                List<Coordinate> coordinates = new ArrayList<>();
                for (int i = 0; i < multiPoint.size(); i++) {
                    Point p = multiPoint.get(i);
                    coordinates.add(new Coordinate(p.getLon(), p.getLat(), p.getAlt()));
                }
                return new MultiPointBuilder(coordinates);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiPolygon multiPolygon) {
                MultiPolygonBuilder polygons = new MultiPolygonBuilder();
                for (int i = 0; i < multiPolygon.size(); i++) {
                    polygons.polygon((PolygonBuilder) visit(multiPolygon.get(i)));
                }
                return polygons;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Point point) {
                return new PointBuilder(point.getLon(), point.getLat());
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(org.elasticsearch.geo.geometry.Polygon polygon) {
                PolygonBuilder polygonBuilder =
                    new PolygonBuilder((LineStringBuilder) visit((org.elasticsearch.geo.geometry.Line) polygon.getPolygon()),
                        ShapeBuilder.Orientation.RIGHT, false);
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    polygonBuilder.hole((LineStringBuilder) visit((org.elasticsearch.geo.geometry.Line) polygon.getHole(i)));
                }
                return polygonBuilder;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Rectangle rectangle) {
                return new EnvelopeBuilder(new Coordinate(rectangle.getMinLon(), rectangle.getMaxLat()),
                    new Coordinate(rectangle.getMaxLon(), rectangle.getMinLat()));
            }
        });
        return shapeBuilder;
    }

    private class ShapeVisitor implements GeometryVisitor<Query, RuntimeException> {
        QueryShardContext context;
        MappedFieldType fieldType;

        ShapeVisitor(QueryShardContext context) {
            this.context = context;
            this.fieldType = context.fieldMapper(fieldName);
        }

        @Override
        public Query visit(Circle circle) {
            throw new QueryShardException(context, "Field [" + fieldName + "] found and unknown shape Circle");
        }

        @Override
        public Query visit(GeometryCollection<?> collection) {
            BooleanQuery.Builder bqb = new BooleanQuery.Builder();
            visit(bqb, collection);
            return bqb.build();
        }

        private void visit(BooleanQuery.Builder bqb, GeometryCollection<?> collection) {
            for (Geometry shape : collection) {
                if (shape instanceof MultiPoint) {
                    // Flatten multipoints
                    visit(bqb, (GeometryCollection<?>) shape);
                } else {
                    bqb.add(shape.visit(this), BooleanClause.Occur.SHOULD);
                }
            }
        }

        @Override
        public Query visit(org.elasticsearch.geo.geometry.Line line) {
            validateIsGeoShapeFieldType();
            return LatLonShape.newLineQuery(fieldName(), relation.getLuceneRelation(), new Line(line.getLats(), line.getLons()));
        }

        @Override
        public Query visit(LinearRing ring) {
            throw new QueryShardException(context, "Field [" + fieldName + "] found and unsupported shape LinearRing");
        }

        @Override
        public Query visit(MultiLine multiLine) {
            validateIsGeoShapeFieldType();
            Line[] lines = new Line[multiLine.size()];
            for (int i=0; i<multiLine.size(); i++) {
                lines[i] = new Line(multiLine.get(i).getLats(), multiLine.get(i).getLons());
            }
            return LatLonShape.newLineQuery(fieldName(), relation.getLuceneRelation(), lines);
        }

        @Override
        public Query visit(MultiPoint multiPoint) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support " + GeoShapeType.MULTIPOINT +
                " queries");
        }

        @Override
        public Query visit(MultiPolygon multiPolygon) {
            Polygon[] polygons = new Polygon[multiPolygon.size()];
            for (int i=0; i<multiPolygon.size(); i++) {
                polygons[i] = toLucenePolygon(multiPolygon.get(i));
            }
            return LatLonShape.newPolygonQuery(fieldName(), relation.getLuceneRelation(), polygons);
        }

        @Override
        public Query visit(Point point) {
            validateIsGeoShapeFieldType();
            return LatLonShape.newBoxQuery(fieldName, relation.getLuceneRelation(),
                point.getLat(), point.getLat(), point.getLon(), point.getLon());
        }

        @Override
        public Query visit(org.elasticsearch.geo.geometry.Polygon polygon) {
            return LatLonShape.newPolygonQuery(fieldName(), relation.getLuceneRelation(), toLucenePolygon(polygon));
        }

        @Override
        public Query visit(org.elasticsearch.geo.geometry.Rectangle r) {
            return LatLonShape.newBoxQuery(fieldName(), relation.getLuceneRelation(),
                r.getMinLat(), r.getMaxLat(), r.getMinLon(), r.getMaxLon());
        }

        private void validateIsGeoShapeFieldType() {
            if (fieldType instanceof GeoShapeFieldMapper.GeoShapeFieldType == false) {
                throw new QueryShardException(context, "Expected " + GeoShapeFieldMapper.CONTENT_TYPE
                    + " field type for Field [" + fieldName + "] but found " + fieldType.typeName());
            }
        }
    }

    @Override
    protected boolean doEquals(GeoShapeQueryBuilder other) {
        return super.doEquals((AbstractGeometryQueryBuilder)other)
            && Objects.equals(strategy, other.strategy);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode(), strategy);
    }

    @Override
    protected GeoShapeQueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        GeoShapeQueryBuilder builder = (GeoShapeQueryBuilder)super.doRewrite(queryRewriteContext);
        builder.strategy(strategy);
        return builder;
    }

    private static class ParsedGeoShapeQueryParams extends ParsedShapeQueryParams {
        SpatialStrategy strategy;

        @Override
        protected boolean parseXContentField(XContentParser parser) throws IOException {
            SpatialStrategy strategy;
            if (SHAPE_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                this.shape = ShapeParser.parse(parser);
                return true;
            } else if (STRATEGY_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                String strategyName = parser.text();
                strategy = SpatialStrategy.fromString(strategyName);
                if (strategy == null) {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown strategy [" + strategyName + " ]");
                } else {
                    this.strategy = strategy;
                }
                return true;
            }
            return false;
        }
    }

    public static GeoShapeQueryBuilder fromXContent(XContentParser parser) throws IOException {
        ParsedGeoShapeQueryParams pgsqp =
            (ParsedGeoShapeQueryParams) AbstractGeometryQueryBuilder.parsedParamsFromXContent(parser, new ParsedGeoShapeQueryParams());

        GeoShapeQueryBuilder builder;
        if (pgsqp.type != null) {
            deprecationLogger.deprecatedAndMaybeLog("geo_share_query_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        if (pgsqp.shape != null) {
            builder = new GeoShapeQueryBuilder(pgsqp.fieldName, pgsqp.shape);
        } else {
            builder = new GeoShapeQueryBuilder(pgsqp.fieldName, pgsqp.id, pgsqp.type);
        }

        if (pgsqp.index != null) {
            builder.indexedShapeIndex(pgsqp.index);
        }

        if (pgsqp.shapePath != null) {
            builder.indexedShapePath(pgsqp.shapePath);
        }

        if (pgsqp.shapeRouting != null) {
            builder.indexedShapeRouting(pgsqp.shapeRouting);
        }

        if (pgsqp.relation != null) {
            builder.relation(pgsqp.relation);
        }

        if (pgsqp.strategy != null) {
            builder.strategy(pgsqp.strategy);
        }

        if (pgsqp.queryName != null) {
            builder.queryName(pgsqp.queryName);
        }

        builder.boost(pgsqp.boost);
        builder.ignoreUnmapped(pgsqp.ignoreUnmapped);
        return builder;
    }
}
