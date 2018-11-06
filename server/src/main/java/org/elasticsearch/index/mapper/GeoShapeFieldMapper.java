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
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.XShapeCollection;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.GeoPointFieldMapper.Names.IGNORE_MALFORMED;

/**
 * FieldMapper for indexing {@link org.locationtech.spatial4j.shape.Shape}s.
 * <p>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.elasticsearch.index.query.GeoShapeQueryBuilder}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 */
public class GeoShapeFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "geo_shape";

    public static class Names {
        public static final String TREE = "tree";
        public static final String TREE_GEOHASH = "geohash";
        public static final String TREE_QUADTREE = "quadtree";
        public static final String TREE_LEVELS = "tree_levels";
        public static final String TREE_PRESISION = "precision";
        public static final String DISTANCE_ERROR_PCT = "distance_error_pct";
        public static final String ORIENTATION = "orientation";
        public static final String STRATEGY = "strategy";
        public static final String STRATEGY_POINTS_ONLY = "points_only";
        public static final String COERCE = "coerce";
    }

    public static class Defaults {
        public static final String TREE = "NONE";
        public static final boolean POINTS_ONLY = false;
        public static final int GEOHASH_LEVELS = GeoUtils.geoHashLevelsForPrecision("50m");
        public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision("50m");
        public static final Orientation ORIENTATION = Orientation.RIGHT;
        public static final double LEGACY_DISTANCE_ERROR_PCT = 0.025d;
        public static final Explicit<SpatialStrategy> STRATEGY = new Explicit<>(SpatialStrategy.VECTOR, false);
        public static final Explicit<Boolean> COERCE = new Explicit<>(false, false);
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Boolean> IGNORE_Z_VALUE = new Explicit<>(true, false);
    }

    public static class Builder extends FieldMapper.Builder<Builder, GeoShapeFieldMapper> {

        private Boolean coerce;
        private Boolean ignoreMalformed;
        private Boolean ignoreZValue;

        public Builder(String name) {
            super(name, new GeoShapeFieldType(), new GeoShapeFieldType());
        }

        @Override
        public GeoShapeFieldType fieldType() {
            return (GeoShapeFieldType)fieldType;
        }

        public Builder coerce(boolean coerce) {
            this.coerce = coerce;
            return this;
        }

        @Override
        protected boolean defaultDocValues(Version indexCreated) {
            return false;
        }

        protected Explicit<Boolean> coerce(BuilderContext context) {
            if (coerce != null) {
                return new Explicit<>(coerce, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(COERCE_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.COERCE;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return this;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        protected Explicit<Boolean> ignoreZValue(BuilderContext context) {
            if (ignoreZValue != null) {
                return new Explicit<>(ignoreZValue, true);
            }
            return Defaults.IGNORE_Z_VALUE;
        }

        public Builder ignoreZValue(final boolean ignoreZValue) {
            this.ignoreZValue = ignoreZValue;
            return this;
        }

        private void setupPrefixTrees() {
            SpatialPrefixTree prefixTree;
            String tree = fieldType().tree().equals("NONE") ? "quadtree" : fieldType().tree();
            int treeLevels = fieldType().treeLevels();
            double precisionInMeters = fieldType().precisionInMeters();
            if ("geohash".equals(tree)) {
                prefixTree = new GeohashPrefixTree(ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(treeLevels, precisionInMeters, Defaults.GEOHASH_LEVELS, true));
            } else if ("legacyquadtree".equals(tree)) {
                prefixTree = new QuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(treeLevels, precisionInMeters, Defaults.QUADTREE_LEVELS, false));
            } else if ("quadtree".equals(tree)) {
                prefixTree = new PackedQuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(treeLevels, precisionInMeters, Defaults.QUADTREE_LEVELS, false));
            } else {
                throw new IllegalArgumentException("Unknown prefix tree type [" + tree + "]");
            }

            RecursivePrefixTreeStrategy rpts = new RecursivePrefixTreeStrategy(prefixTree, name());
            rpts.setDistErrPct(fieldType().distanceErrorPct());
            rpts.setPruneLeafyBranches(false);
            fieldType().recursiveStrategy = rpts;

            TermQueryPrefixTreeStrategy termStrategy = new TermQueryPrefixTreeStrategy(prefixTree, name());
            termStrategy.setDistErrPct(fieldType().distanceErrorPct());
            fieldType().termStrategy = termStrategy;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            // field mapper handles this at build time
            // but prefix tree strategies require a name, so throw a similar exception
            if (name().isEmpty()) {
                throw new IllegalArgumentException("name cannot be empty string");
            }

            // throw an exception if spatial strategy is set to vector and a "tree" was explicitly set
            if (fieldType().strategy() == SpatialStrategy.VECTOR && fieldType().tree().equals("NONE") == false) {
                throw new IllegalArgumentException("prefix trees cannot be used when index strategy is set to ["
                    + SpatialStrategy.VECTOR + "]");
            }

            // setup prefix trees regardless of strategy (this is used for the QueryBuilder)
            setupPrefixTrees();

            // set the default PrefixTree Strategy if the spatial strategy is not set to "VECTOR"
            if (fieldType().strategy() != SpatialStrategy.VECTOR) {
                fieldType().defaultPrefixTreeStrategy = fieldType().resolvePrefixTreeStrategy(fieldType().strategy());
                fieldType().defaultPrefixTreeStrategy.setPointsOnly(fieldType().pointsOnly());
            }
        }

        private static int getLevels(int treeLevels, double precisionInMeters, int defaultLevels, boolean geoHash) {
            if (treeLevels > 0 || precisionInMeters >= 0) {
                return Math.max(treeLevels, precisionInMeters >= 0 ? (geoHash ? GeoUtils.geoHashLevelsForPrecision(precisionInMeters)
                    : GeoUtils.quadTreeLevelsForPrecision(precisionInMeters)) : 0);
            }
            return defaultLevels;
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            GeoShapeFieldType geoShapeFieldType = (GeoShapeFieldType)fieldType;

            if (geoShapeFieldType.treeLevels() == 0 && geoShapeFieldType.precisionInMeters() < 0) {
                geoShapeFieldType.setDefaultDistanceErrorPct(Defaults.LEGACY_DISTANCE_ERROR_PCT);
            }
            setupFieldType(context);

            return new GeoShapeFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                    ignoreZValue(context), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            Boolean pointsOnly = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (Names.TREE.equals(fieldName)) {
                    checkPrefixTreeSupport(fieldName);
                    builder.fieldType().setTree(fieldNode.toString());
                    // explicitly set strategy
                    if (builder.fieldType().strategy() == Defaults.STRATEGY.value()) {
                        builder.fieldType().setStrategy(SpatialStrategy.RECURSIVE);
                    }
                    iterator.remove();
                } else if (Names.TREE_LEVELS.equals(fieldName)) {
                    checkPrefixTreeSupport(fieldName);
                    builder.fieldType().setTreeLevels(Integer.parseInt(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.TREE_PRESISION.equals(fieldName)) {
                    builder.fieldType().setPrecisionInMeters(DistanceUnit.parse(fieldNode.toString(),
                        DistanceUnit.DEFAULT, DistanceUnit.DEFAULT));
                    iterator.remove();
                } else if (Names.DISTANCE_ERROR_PCT.equals(fieldName)) {
                    checkPrefixTreeSupport(fieldName);
                    builder.fieldType().setDistanceErrorPct(Double.parseDouble(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.ORIENTATION.equals(fieldName)) {
                    builder.fieldType().setOrientation(ShapeBuilder.Orientation.fromString(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.STRATEGY.equals(fieldName)) {
                    SpatialStrategy strategy = SpatialStrategy.fromString(fieldNode.toString());
                    String prefixTree = builder.fieldType().tree();
                    if (strategy == SpatialStrategy.VECTOR && prefixTree.equals("NONE") == false) {
                        throw new ElasticsearchParseException("Strategy [{}] cannot be used with PrefixTree [{}]", strategy, prefixTree);
                    }
                    builder.fieldType().setStrategy(strategy);
                    iterator.remove();
                } else if (IGNORE_MALFORMED.equals(fieldName)) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(fieldNode, name + ".ignore_malformed"));
                    iterator.remove();
                } else if (Names.COERCE.equals(fieldName)) {
                    builder.coerce(XContentMapValues.nodeBooleanValue(fieldNode, name + "." + Names.COERCE));
                    iterator.remove();
                } else if (GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName().equals(fieldName)) {
                    builder.ignoreZValue(XContentMapValues.nodeBooleanValue(fieldNode,
                        name + "." + GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName()));
                    iterator.remove();
                } else if (Names.STRATEGY_POINTS_ONLY.equals(fieldName)) {
                    pointsOnly = XContentMapValues.nodeBooleanValue(fieldNode, name + "." + Names.STRATEGY_POINTS_ONLY);
                    iterator.remove();
                }
            }
            if (pointsOnly != null) {
                if (builder.fieldType().strategy == SpatialStrategy.TERM && pointsOnly == false) {
                    throw new IllegalArgumentException("points_only cannot be set to false for term strategy");
                } else {
                    builder.fieldType().setPointsOnly(pointsOnly);
                }
            }
            return builder;
        }

        private void checkPrefixTreeSupport(String fieldName) {
            if (ShapesAvailability.JTS_AVAILABLE == false || ShapesAvailability.SPATIAL4J_AVAILABLE == false) {
                throw new ElasticsearchParseException("Field parameter [{}] is not supported for [{}] field type", fieldName, CONTENT_TYPE);
            }
        }
    }

    public static final class GeoShapeFieldType extends MappedFieldType {

        private String tree = Defaults.TREE;
        private SpatialStrategy strategy = Defaults.STRATEGY.value();
        private boolean pointsOnly = Defaults.POINTS_ONLY;
        private int treeLevels = 0;
        private double precisionInMeters = -1;
        private Double distanceErrorPct;
        private double defaultDistanceErrorPct = 0.0;
        private Orientation orientation = Defaults.ORIENTATION;

        // these are built when the field type is frozen
        private PrefixTreeStrategy defaultPrefixTreeStrategy;
        private RecursivePrefixTreeStrategy recursiveStrategy;
        private TermQueryPrefixTreeStrategy termStrategy;

        public GeoShapeFieldType() {
            setIndexOptions(IndexOptions.DOCS);
            setTokenized(false);
            setStored(false);
            setStoreTermVectors(false);
            setOmitNorms(true);
        }

        protected GeoShapeFieldType(GeoShapeFieldType ref) {
            super(ref);
            this.tree = ref.tree;
            this.strategy = ref.strategy;
//            this.strategyName = ref.strategyName;
            this.pointsOnly = ref.pointsOnly;
            this.treeLevels = ref.treeLevels;
            this.precisionInMeters = ref.precisionInMeters;
            this.distanceErrorPct = ref.distanceErrorPct;
            this.defaultDistanceErrorPct = ref.defaultDistanceErrorPct;
            this.orientation = ref.orientation;
        }

        @Override
        public GeoShapeFieldType clone() {
            return new GeoShapeFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            GeoShapeFieldType that = (GeoShapeFieldType) o;
            return treeLevels == that.treeLevels &&
                precisionInMeters == that.precisionInMeters &&
                defaultDistanceErrorPct == that.defaultDistanceErrorPct &&
                Objects.equals(tree, that.tree) &&
                Objects.equals(strategy, that.strategy) &&
//                Objects.equals(strategyName, that.strategyName) &&
                pointsOnly == that.pointsOnly &&
                Objects.equals(distanceErrorPct, that.distanceErrorPct) &&
                orientation == that.orientation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), tree, strategy, pointsOnly, treeLevels, precisionInMeters, distanceErrorPct,
                    defaultDistanceErrorPct, orientation);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts) {
            super.checkCompatibility(fieldType, conflicts);
            GeoShapeFieldType other = (GeoShapeFieldType)fieldType;
            // prevent user from changing strategies
            if (strategy() != other.strategy()) {
                conflicts.add("mapper [" + name() + "] has different [strategy]");
            }

            // prevent user from changing trees (changes encoding)
            if (tree().equals(other.tree()) == false) {
                conflicts.add("mapper [" + name() + "] has different [tree]");
            }

            if ((pointsOnly() != other.pointsOnly())) {
                conflicts.add("mapper [" + name() + "] has different points_only");
            }

            // TODO we should allow this, but at the moment levels is used to build bookkeeping variables
            // in lucene's SpatialPrefixTree implementations, need a patch to correct that first
            if (treeLevels() != other.treeLevels()) {
                conflicts.add("mapper [" + name() + "] has different [tree_levels]");
            }
            if (precisionInMeters() != other.precisionInMeters()) {
                conflicts.add("mapper [" + name() + "] has different [precision]");
            }
        }

        private static int getLevels(int treeLevels, double precisionInMeters, int defaultLevels, boolean geoHash) {
            if (treeLevels > 0 || precisionInMeters >= 0) {
                return Math.max(treeLevels, precisionInMeters >= 0 ? (geoHash ? GeoUtils.geoHashLevelsForPrecision(precisionInMeters)
                    : GeoUtils.quadTreeLevelsForPrecision(precisionInMeters)) : 0);
            }
            return defaultLevels;
        }

        public String tree() {
            return tree;
        }

        public void setTree(String tree) {
            checkIfFrozen();
            this.tree = tree;
        }

        public SpatialStrategy strategy() {
            return strategy;
        }

        public void setStrategy(SpatialStrategy strategy) {
            checkIfFrozen();
            this.strategy = strategy;
            if (this.strategy.equals(SpatialStrategy.TERM)) {
                this.pointsOnly = true;
            }
        }

        public boolean pointsOnly() {
            return pointsOnly;
        }

        public void setPointsOnly(boolean pointsOnly) {
            checkIfFrozen();
            this.pointsOnly = pointsOnly;
        }
        public int treeLevels() {
            return treeLevels;
        }

        public void setTreeLevels(int treeLevels) {
            checkIfFrozen();
            this.treeLevels = treeLevels;
        }

        public double precisionInMeters() {
            return precisionInMeters;
        }

        public void setPrecisionInMeters(double precisionInMeters) {
            checkIfFrozen();
            this.precisionInMeters = precisionInMeters;
        }

        public double distanceErrorPct() {
            return distanceErrorPct == null ? defaultDistanceErrorPct : distanceErrorPct;
        }

        public void setDistanceErrorPct(double distanceErrorPct) {
            checkIfFrozen();
            this.distanceErrorPct = distanceErrorPct;
        }

        public void setDefaultDistanceErrorPct(double defaultDistanceErrorPct) {
            checkIfFrozen();
            this.defaultDistanceErrorPct = defaultDistanceErrorPct;
        }

        public Orientation orientation() { return this.orientation; }

        public void setOrientation(Orientation orientation) {
            checkIfFrozen();
            this.orientation = orientation;
        }

        public PrefixTreeStrategy defaultPrefixTreeStrategy() {
            return this.defaultPrefixTreeStrategy;
        }

        public PrefixTreeStrategy resolvePrefixTreeStrategy(SpatialStrategy strategy) {
            return resolvePrefixTreeStrategy(strategy.getStrategyName());
        }

        public PrefixTreeStrategy resolvePrefixTreeStrategy(String strategyName) {
            if (SpatialStrategy.RECURSIVE.getStrategyName().equals(strategyName)) {
                return recursiveStrategy;
            }
            if (SpatialStrategy.TERM.getStrategyName().equals(strategyName)) {
                return termStrategy;
            }
            throw new IllegalArgumentException("Unknown prefix tree strategy [" + strategyName + "]");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Geo fields do not support exact searching, use dedicated geo queries instead");
        }
    }

    protected Explicit<Boolean> coerce;
    protected Explicit<Boolean> ignoreMalformed;
    protected Explicit<Boolean> ignoreZValue;

    public GeoShapeFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                               Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                               Explicit<Boolean> ignoreZValue, Settings indexSettings,
                               MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.coerce = coerce;
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreZValue = ignoreZValue;
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    public void parseForInvertedIndexing(ParseContext context) throws IOException {
        try {
            Shape shape = context.parseExternalValue(Shape.class);
            if (shape == null) {
                ShapeBuilder shapeBuilder = ShapeParser.parse(context.parser(), this);
                if (shapeBuilder == null) {
                    return;
                }
                shape = shapeBuilder.buildS4J();
            }
            if (fieldType().pointsOnly() == true) {
                // index configured for pointsOnly
                if (shape instanceof XShapeCollection && XShapeCollection.class.cast(shape).pointsOnly()) {
                    // MULTIPOINT data: index each point separately
                    List<Shape> shapes = ((XShapeCollection) shape).getShapes();
                    for (Shape s : shapes) {
                        indexShape(context, s);
                    }
                    return;
                } else if (shape instanceof Point == false) {
                    throw new MapperParsingException("[{" + fieldType().name() + "}] is configured for points only but a "
                        + ((shape instanceof JtsGeometry) ? ((JtsGeometry)shape).getGeom().getGeometryType() : shape.getClass())
                        + " was found");
                }
            }
            indexShape(context, shape);
        } catch (Exception e) {
            if (ignoreMalformed.value() == false) {
                throw new MapperParsingException("failed to parse field [{}] of type [{}]", e, fieldType().name(),
                    fieldType().typeName());
            }
            context.addIgnoredField(fieldType.name());
        }
    }

    /** parsing logic for {@link LatLonShape} indexing */
    public void parseForPointsIndexing(ParseContext context) throws IOException {
        try {
            Object shape = context.parseExternalValue(Object.class);
            if (shape == null) {
                ShapeBuilder shapeBuilder = ShapeParser.parse(context.parser(), this);
                if (shapeBuilder == null) {
                    return;
                }
                shape = shapeBuilder.buildLucene();
            }
            indexShape(context, shape);
        } catch (Exception e) {
            if (ignoreMalformed.value() == false) {
                throw new MapperParsingException("failed to parse field [{}] of type [{}]", e, fieldType().name(),
                    fieldType().typeName());
            }
            context.addIgnoredField(fieldType().name());
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (fieldType().strategy() == SpatialStrategy.VECTOR) {
            parseForPointsIndexing(context);
        } else {
            parseForInvertedIndexing(context);
        }
    }

    private void indexShape(ParseContext context, Shape shape) {
        List<IndexableField> fields = new ArrayList<>(Arrays.asList(fieldType().defaultPrefixTreeStrategy().createIndexableFields(shape)));
        createFieldNamesField(context, fields);
        for (IndexableField field : fields) {
            context.doc().add(field);
        }
    }

    private void indexShape(ParseContext context, Object luceneShape) {
        if (luceneShape instanceof GeoPoint) {
            GeoPoint pt = (GeoPoint)luceneShape;
            indexFields(context, LatLonShape.createIndexableFields(name(), pt.lat(), pt.lon()));
        } else if (luceneShape instanceof Line) {
            indexFields(context, LatLonShape.createIndexableFields(name(), (Line)luceneShape));
        } else if (luceneShape instanceof Polygon) {
            indexFields(context, LatLonShape.createIndexableFields(name(), (Polygon) luceneShape));
        } else if (luceneShape instanceof double[][]) {
            double[][] pts = (double[][])luceneShape;
            for (int i = 0; i < pts.length; ++i) {
                indexFields(context, LatLonShape.createIndexableFields(name(), pts[i][1], pts[i][0]));
            }
        } else if (luceneShape instanceof Line[]) {
            Line[] lines = (Line[]) luceneShape;
            for (int i = 0; i < lines.length; ++i) {
                indexFields(context, LatLonShape.createIndexableFields(name(), lines[i]));
            }
        } else if (luceneShape instanceof Polygon[]) {
            Polygon[] polys = (Polygon[]) luceneShape;
            for (int i = 0; i < polys.length; ++i) {
                indexFields(context, LatLonShape.createIndexableFields(name(), polys[i]));
            }
        } else if (luceneShape instanceof Rectangle) {
            // index rectangle as a polygon
            Rectangle r = (Rectangle) luceneShape;
            Polygon p = new Polygon(new double[]{r.minLat, r.minLat, r.maxLat, r.maxLat, r.minLat},
                new double[]{r.minLon, r.maxLon, r.maxLon, r.minLon, r.minLon});
            indexFields(context, LatLonShape.createIndexableFields(name(), p));
        } else if (luceneShape instanceof Object[]) {
            // recurse to index geometry collection
            for (Object o : (Object[])luceneShape) {
                indexShape(context, o);
            }
        } else {
            throw new IllegalArgumentException("invalid shape type found [" + luceneShape.getClass().getName() + "] while indexing shape");
        }
    }

    private void indexFields(ParseContext context, Field[] fields) {
        for (Field f : fields) {
            context.doc().add(f);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);

        GeoShapeFieldMapper gsfm = (GeoShapeFieldMapper)mergeWith;
        if (gsfm.coerce.explicit()) {
            this.coerce = gsfm.coerce;
        }
        if (gsfm.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gsfm.ignoreMalformed;
        }
        if (gsfm.ignoreZValue.explicit()) {
            this.ignoreZValue = gsfm.ignoreZValue;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());

        if (includeDefaults || fieldType().tree().equals(Defaults.TREE) == false) {
            builder.field(Names.TREE, fieldType().tree());
        }

        if (fieldType().treeLevels() != 0) {
            builder.field(Names.TREE_LEVELS, fieldType().treeLevels());
        } else if(includeDefaults && fieldType().precisionInMeters() == -1) { // defaults only make sense if precision is not specified
            if ("geohash".equals(fieldType().tree())) {
                builder.field(Names.TREE_LEVELS, Defaults.GEOHASH_LEVELS);
            } else if ("legacyquadtree".equals(fieldType().tree())) {
                builder.field(Names.TREE_LEVELS, Defaults.QUADTREE_LEVELS);
            } else if ("quadtree".equals(fieldType().tree())) {
                builder.field(Names.TREE_LEVELS, Defaults.QUADTREE_LEVELS);
            } else {
                throw new IllegalArgumentException("Unknown prefix tree type [" + fieldType().tree() + "]");
            }
        }
        if (fieldType().precisionInMeters() != -1) {
            builder.field(Names.TREE_PRESISION, DistanceUnit.METERS.toString(fieldType().precisionInMeters()));
        } else if (includeDefaults && fieldType().treeLevels() == 0) { // defaults only make sense if tree levels are not specified
            builder.field(Names.TREE_PRESISION, DistanceUnit.METERS.toString(50));
        }
        if (includeDefaults || fieldType().strategy() != Defaults.STRATEGY.value()) {
            builder.field(Names.STRATEGY, fieldType().strategy().getStrategyName());
        }
        if (includeDefaults || fieldType().distanceErrorPct() != fieldType().defaultDistanceErrorPct) {
            builder.field(Names.DISTANCE_ERROR_PCT, fieldType().distanceErrorPct());
        }
        if (includeDefaults || fieldType().orientation() != Defaults.ORIENTATION) {
            builder.field(Names.ORIENTATION, fieldType().orientation());
        }
        if (fieldType().strategy() == SpatialStrategy.TERM) {
            // For TERMs strategy the defaults for points only change to true
            if (includeDefaults || fieldType().pointsOnly() != true) {
                builder.field(Names.STRATEGY_POINTS_ONLY, fieldType().pointsOnly());
            }
        } else {
            if (includeDefaults || fieldType().pointsOnly() != GeoShapeFieldMapper.Defaults.POINTS_ONLY) {
                builder.field(Names.STRATEGY_POINTS_ONLY, fieldType().pointsOnly());
            }
        }
        if (includeDefaults || coerce.explicit()) {
            builder.field(Names.COERCE, coerce.value());
        }
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(IGNORE_MALFORMED, ignoreMalformed.value());
        }
        if (includeDefaults || ignoreZValue.explicit()) {
            builder.field(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName(), ignoreZValue.value());
        }
    }

    public Explicit<Boolean> coerce() {
        return coerce;
    }

    public Explicit<Boolean> ignoreMalformed() {
        return ignoreMalformed;
    }

    public Explicit<Boolean> ignoreZValue() {
        return ignoreZValue;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
