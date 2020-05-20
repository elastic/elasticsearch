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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.LegacyGeoShapeQueryProcessor;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

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
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))
 *
 * @deprecated use {@link GeoShapeFieldMapper}
 */
@Deprecated
public class LegacyGeoShapeFieldMapper extends AbstractShapeGeometryFieldMapper<ShapeBuilder<?, ?, ?>, Shape> {

    public static final String CONTENT_TYPE = "geo_shape";

    @Deprecated
    public static class DeprecatedParameters {
        public static class Names {
            public static final ParseField STRATEGY = new ParseField("strategy");
            public static final ParseField TREE = new ParseField("tree");
            public static final ParseField TREE_LEVELS = new ParseField("tree_levels");
            public static final ParseField PRECISION = new ParseField("precision");
            public static final ParseField DISTANCE_ERROR_PCT = new ParseField("distance_error_pct");
            public static final ParseField POINTS_ONLY = new ParseField("points_only");
        }

        public static class PrefixTrees {
            public static final String LEGACY_QUADTREE = "legacyquadtree";
            public static final String QUADTREE = "quadtree";
            public static final String GEOHASH = "geohash";
        }

        public static class Defaults {
            public static final SpatialStrategy STRATEGY = SpatialStrategy.RECURSIVE;
            public static final String TREE = "quadtree";
            public static final String PRECISION = "50m";
            public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision(PRECISION);
            public static final int GEOHASH_TREE_LEVELS = GeoUtils.geoHashLevelsForPrecision(PRECISION);
            public static final boolean POINTS_ONLY = false;
            public static final double DISTANCE_ERROR_PCT = 0.025d;
        }

        public SpatialStrategy strategy = null;
        public String tree = null;
        public Integer treeLevels = null;
        public String precision = null;
        public Boolean pointsOnly = null;
        public Double distanceErrorPct = null;

        public void setSpatialStrategy(SpatialStrategy strategy) {
            this.strategy = strategy;
        }

        public void setTree(String prefixTree) {
            this.tree = prefixTree;
        }

        public void setTreeLevels(int treeLevels) {
            this.treeLevels = treeLevels;
        }

        public void setPrecision(String precision) {
            this.precision = precision;
        }

        public void setPointsOnly(boolean pointsOnly) {
            if (this.strategy == SpatialStrategy.TERM && pointsOnly == false) {
                throw new ElasticsearchParseException("points_only cannot be set to false for term strategy");
            }
            this.pointsOnly = pointsOnly;
        }

        public void setDistanceErrorPct(double distanceErrorPct) {
            this.distanceErrorPct = distanceErrorPct;
        }

        public static boolean parse(String name, String fieldName, Object fieldNode, DeprecatedParameters deprecatedParameters) {
            if (Names.STRATEGY.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setSpatialStrategy(SpatialStrategy.fromString(fieldNode.toString()));
            } else if (Names.TREE.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setTree(fieldNode.toString());
            } else if (Names.TREE_LEVELS.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setTreeLevels(Integer.parseInt(fieldNode.toString()));
            } else if (Names.PRECISION.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setPrecision(fieldNode.toString());
            } else if (Names.DISTANCE_ERROR_PCT.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setDistanceErrorPct(Double.parseDouble(fieldNode.toString()));
            } else if (Names.POINTS_ONLY.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setPointsOnly(
                    XContentMapValues.nodeBooleanValue(fieldNode, name + "." + DeprecatedParameters.Names.POINTS_ONLY));
            } else {
                return false;
            }
            return true;
        }

        private static void checkPrefixTreeSupport(String fieldName) {
            if (ShapesAvailability.JTS_AVAILABLE == false || ShapesAvailability.SPATIAL4J_AVAILABLE == false) {
                throw new ElasticsearchParseException("Field parameter [{}] is not supported for [{}] field type",
                    fieldName, CONTENT_TYPE);
            }
            DEPRECATION_LOGGER.deprecatedAndMaybeLog("geo_mapper_field_parameter",
                "Field parameter [{}] is deprecated and will be removed in a future version.", fieldName);
        }
    }

    private static final Logger logger = LogManager.getLogger(LegacyGeoShapeFieldMapper.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(logger);

    public static class Builder extends AbstractShapeGeometryFieldMapper.Builder<AbstractShapeGeometryFieldMapper.Builder,
        LegacyGeoShapeFieldMapper.GeoShapeFieldType> {

        DeprecatedParameters deprecatedParameters;

        public Builder(String name) {
            this(name, new DeprecatedParameters());
        }

        public Builder(String name, DeprecatedParameters deprecatedParameters) {
            super(name, new GeoShapeFieldType(), new GeoShapeFieldType());
            this.deprecatedParameters = deprecatedParameters;
        }

        @Override
        protected void setGeometryParser(GeoShapeFieldType fieldType) {
            fieldType().setGeometryParser(ShapeParser::parse);
        }

        @Override
        public void setGeometryIndexer(LegacyGeoShapeFieldMapper.GeoShapeFieldType fieldType) {
            fieldType().setGeometryIndexer(new LegacyGeoShapeIndexer(fieldType));
        }

        @Override
        protected void setGeometryQueryBuilder(GeoShapeFieldType fieldType) {
            fieldType().setGeometryQueryBuilder(new LegacyGeoShapeQueryProcessor(fieldType()));
        }

        private void setupFieldTypeDeprecatedParameters(BuilderContext context) {
            GeoShapeFieldType ft = fieldType();
            if (deprecatedParameters.strategy != null) {
                ft.setStrategy(deprecatedParameters.strategy);
            }
            if (deprecatedParameters.tree != null) {
                ft.setTree(deprecatedParameters.tree);
            }
            if (deprecatedParameters.treeLevels != null) {
                ft.setTreeLevels(deprecatedParameters.treeLevels);
            }
            if (deprecatedParameters.precision != null) {
                // precision is only set iff: a. treeLevel is not explicitly set, b. its explicitly set
                ft.setPrecisionInMeters(DistanceUnit.parse(deprecatedParameters.precision,
                    DistanceUnit.DEFAULT, DistanceUnit.DEFAULT));
            }
            if (deprecatedParameters.distanceErrorPct != null) {
                ft.setDistanceErrorPct(deprecatedParameters.distanceErrorPct);
            }
            if (deprecatedParameters.pointsOnly != null) {
                ft.setPointsOnly(deprecatedParameters.pointsOnly);
            }

            GeoShapeFieldType geoShapeFieldType = (GeoShapeFieldType)fieldType;

            if (geoShapeFieldType.treeLevels() == 0 && geoShapeFieldType.precisionInMeters() < 0) {
                geoShapeFieldType.setDefaultDistanceErrorPct(DeprecatedParameters.Defaults.DISTANCE_ERROR_PCT);
            }
        }

        private void setupPrefixTrees() {
            GeoShapeFieldType ft = fieldType();
            SpatialPrefixTree prefixTree;
            if (ft.tree().equals(DeprecatedParameters.PrefixTrees.GEOHASH)) {
                prefixTree = new GeohashPrefixTree(ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(ft.treeLevels(), ft.precisionInMeters(), DeprecatedParameters.Defaults.GEOHASH_TREE_LEVELS, true));
            } else if (ft.tree().equals(DeprecatedParameters.PrefixTrees.LEGACY_QUADTREE)) {
                prefixTree = new QuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(ft.treeLevels(), ft.precisionInMeters(), DeprecatedParameters.Defaults.QUADTREE_LEVELS, false));
            } else if (ft.tree().equals(DeprecatedParameters.PrefixTrees.QUADTREE)) {
                prefixTree = new PackedQuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(ft.treeLevels(), ft.precisionInMeters(), DeprecatedParameters.Defaults.QUADTREE_LEVELS, false));
            } else {
                throw new IllegalArgumentException("Unknown prefix tree type [" + ft.tree() + "]");
            }

            // setup prefix trees regardless of strategy (this is used for the QueryBuilder)
            // recursive:
            RecursivePrefixTreeStrategy rpts = new RecursivePrefixTreeStrategy(prefixTree, ft.name());
            rpts.setDistErrPct(ft.distanceErrorPct());
            rpts.setPruneLeafyBranches(false);
            ft.recursiveStrategy = rpts;

            // term:
            TermQueryPrefixTreeStrategy termStrategy = new TermQueryPrefixTreeStrategy(prefixTree, ft.name());
            termStrategy.setDistErrPct(ft.distanceErrorPct());
            ft.termStrategy = termStrategy;

            // set default (based on strategy):
            ft.defaultPrefixTreeStrategy = ft.resolvePrefixTreeStrategy(ft.strategy());
            ft.defaultPrefixTreeStrategy.setPointsOnly(ft.pointsOnly());
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            // setup the deprecated parameters and the prefix tree configuration
            setupFieldTypeDeprecatedParameters(context);
            setupPrefixTrees();
        }

        private static int getLevels(int treeLevels, double precisionInMeters, int defaultLevels, boolean geoHash) {
            if (treeLevels > 0 || precisionInMeters >= 0) {
                return Math.max(treeLevels, precisionInMeters >= 0 ? (geoHash ? GeoUtils.geoHashLevelsForPrecision(precisionInMeters)
                    : GeoUtils.quadTreeLevelsForPrecision(precisionInMeters)) : 0);
            }
            return defaultLevels;
        }

        @Override
        public LegacyGeoShapeFieldMapper build(BuilderContext context) {
            setupFieldType(context);

            return new LegacyGeoShapeFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                coerce(context), orientation(), ignoreZValue(), context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static final class GeoShapeFieldType extends AbstractShapeGeometryFieldType<ShapeBuilder<?, ?, ?>, Shape> {

        private String tree = DeprecatedParameters.Defaults.TREE;
        private SpatialStrategy strategy = DeprecatedParameters.Defaults.STRATEGY;
        private boolean pointsOnly = DeprecatedParameters.Defaults.POINTS_ONLY;
        private int treeLevels = 0;
        private double precisionInMeters = -1;
        private Double distanceErrorPct;
        private double defaultDistanceErrorPct = 0.0;

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
            this.pointsOnly = ref.pointsOnly;
            this.treeLevels = ref.treeLevels;
            this.precisionInMeters = ref.precisionInMeters;
            this.distanceErrorPct = ref.distanceErrorPct;
            this.defaultDistanceErrorPct = ref.defaultDistanceErrorPct;
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
                pointsOnly == that.pointsOnly &&
                Objects.equals(distanceErrorPct, that.distanceErrorPct);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), tree, strategy, pointsOnly, treeLevels, precisionInMeters, distanceErrorPct,
                    defaultDistanceErrorPct);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
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
    }

    public LegacyGeoShapeFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                               Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce, Explicit<Orientation> orientation,
                               Explicit<Boolean> ignoreZValue, Settings indexSettings,
                               MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, ignoreZValue, orientation, indexSettings,
            multiFields, copyTo);
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    protected void addStoredFields(ParseContext context, Shape geometry) {
        // noop: we do not store geo_shapes; and will not store legacy geo_shape types
    }

    @Override
    protected void addDocValuesFields(String name, Shape geometry, List<IndexableField> fields, ParseContext context) {
        // doc values are not supported
    }

    @Override
    protected void addMultiFields(ParseContext context, Shape geometry) {
        // noop (completion suggester currently not compatible with geo_shape)
    }

    @Override
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults
            || (fieldType().tree().equals(DeprecatedParameters.Defaults.TREE)) == false) {
            builder.field(DeprecatedParameters.Names.TREE.getPreferredName(), fieldType().tree());
        }

        if (fieldType().treeLevels() != 0) {
            builder.field(DeprecatedParameters.Names.TREE_LEVELS.getPreferredName(), fieldType().treeLevels());
        } else if(includeDefaults && fieldType().precisionInMeters() == -1) { // defaults only make sense if precision is not specified
            if (DeprecatedParameters.PrefixTrees.GEOHASH.equals(fieldType().tree())) {
                builder.field(DeprecatedParameters.Names.TREE_LEVELS.getPreferredName(),
                    DeprecatedParameters.Defaults.GEOHASH_TREE_LEVELS);
            } else if (DeprecatedParameters.PrefixTrees.LEGACY_QUADTREE.equals(fieldType().tree())) {
                builder.field(DeprecatedParameters.Names.TREE_LEVELS.getPreferredName(),
                    DeprecatedParameters.Defaults.QUADTREE_LEVELS);
            } else if (DeprecatedParameters.PrefixTrees.QUADTREE.equals(fieldType().tree())) {
                builder.field(DeprecatedParameters.Names.TREE_LEVELS.getPreferredName(),
                    DeprecatedParameters.Defaults.QUADTREE_LEVELS);
            } else {
                throw new IllegalArgumentException("Unknown prefix tree type [" + fieldType().tree() + "]");
            }
        }
        if (fieldType().precisionInMeters() != -1) {
            builder.field(DeprecatedParameters.Names.PRECISION.getPreferredName(),
                DistanceUnit.METERS.toString(fieldType().precisionInMeters()));
        } else if (includeDefaults && fieldType().treeLevels() == 0) { // defaults only make sense if tree levels are not specified
            builder.field(DeprecatedParameters.Names.PRECISION.getPreferredName(),
                DistanceUnit.METERS.toString(50));
        }

        if (indexCreatedVersion.onOrAfter(Version.V_7_0_0)) {
            builder.field(DeprecatedParameters.Names.STRATEGY.getPreferredName(), fieldType().strategy().getStrategyName());
        }

        if (includeDefaults || fieldType().distanceErrorPct() != fieldType().defaultDistanceErrorPct) {
            builder.field(DeprecatedParameters.Names.DISTANCE_ERROR_PCT.getPreferredName(), fieldType().distanceErrorPct());
        }
        if (fieldType().strategy() == SpatialStrategy.TERM) {
            // For TERMs strategy the defaults for points only change to true
            if (includeDefaults || fieldType().pointsOnly() != true) {
                builder.field(DeprecatedParameters.Names.POINTS_ONLY.getPreferredName(), fieldType().pointsOnly());
            }
        } else {
            if (includeDefaults || fieldType().pointsOnly() != DeprecatedParameters.Defaults.POINTS_ONLY) {
                builder.field(DeprecatedParameters.Names.POINTS_ONLY.getPreferredName(), fieldType().pointsOnly());
            }
        }
    }

    @Override
    protected void mergeGeoOptions(AbstractShapeGeometryFieldMapper<?,?> mergeWith, List<String> conflicts) {

        if (mergeWith instanceof GeoShapeFieldMapper) {
            GeoShapeFieldMapper fieldMapper = (GeoShapeFieldMapper) mergeWith;
            throw new IllegalArgumentException("[" + fieldType().name() + "] with field mapper [" + fieldType().typeName() + "] " +
                "using [" + fieldType().strategy() + "] strategy cannot be merged with " + "[" + fieldMapper.typeName() +
                "] with [BKD] strategy");
        }

        GeoShapeFieldType g = (GeoShapeFieldType)mergeWith.fieldType();
        // prevent user from changing strategies
        if (fieldType().strategy() != g.strategy()) {
            conflicts.add("mapper [" + name() + "] has different [strategy]");
        }

        // prevent user from changing trees (changes encoding)
        if (fieldType().tree().equals(g.tree()) == false) {
            conflicts.add("mapper [" + name() + "] has different [tree]");
        }

        if (fieldType().pointsOnly() != g.pointsOnly()) {
            conflicts.add("mapper [" + name() + "] has different points_only");
        }

        // TODO we should allow this, but at the moment levels is used to build bookkeeping variables
        // in lucene's SpatialPrefixTree implementations, need a patch to correct that first
        if (fieldType().treeLevels() != g.treeLevels()) {
            conflicts.add("mapper [" + name() + "] has different [tree_levels]");
        }
        if (fieldType().precisionInMeters() != g.precisionInMeters()) {
            conflicts.add("mapper [" + name() + "] has different [precision]");
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
