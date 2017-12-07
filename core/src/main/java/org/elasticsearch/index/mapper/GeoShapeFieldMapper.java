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
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.XShapeCollection;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
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
        public static final String TREE = Names.TREE_GEOHASH;
        public static final String STRATEGY = SpatialStrategy.RECURSIVE.getStrategyName();
        public static final boolean POINTS_ONLY = false;
        public static final int GEOHASH_LEVELS = GeoUtils.geoHashLevelsForPrecision("50m");
        public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision("50m");
        public static final Orientation ORIENTATION = Orientation.RIGHT;
        public static final double LEGACY_DISTANCE_ERROR_PCT = 0.025d;
        public static final Explicit<Boolean> COERCE = new Explicit<>(false, false);
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);

        public static final MappedFieldType FIELD_TYPE = new GeoShapeFieldType();

        static {
            // setting name here is a hack so freeze can be called...instead all these options should be
            // moved to the default ctor for GeoShapeFieldType, and defaultFieldType() should be removed from mappers...
            FIELD_TYPE.setName("DoesNotExist");
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, GeoShapeFieldMapper> {

        private Boolean coerce;
        private Boolean ignoreMalformed;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
        }

        @Override
        public GeoShapeFieldType fieldType() {
            return (GeoShapeFieldType)fieldType;
        }

        public Builder coerce(boolean coerce) {
            this.coerce = coerce;
            return builder;
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
            return builder;
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

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            GeoShapeFieldType geoShapeFieldType = (GeoShapeFieldType)fieldType;

            if (geoShapeFieldType.treeLevels() == 0 && geoShapeFieldType.precisionInMeters() < 0) {
                geoShapeFieldType.setDefaultDistanceErrorPct(Defaults.LEGACY_DISTANCE_ERROR_PCT);
            }
            setupFieldType(context);

            return new GeoShapeFieldMapper(name, fieldType, ignoreMalformed(context), coerce(context), context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (Names.TREE.equals(fieldName)) {
                    builder.fieldType().setTree(fieldNode.toString());
                    iterator.remove();
                } else if (Names.TREE_LEVELS.equals(fieldName)) {
                    builder.fieldType().setTreeLevels(Integer.parseInt(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.TREE_PRESISION.equals(fieldName)) {
                    builder.fieldType().setPrecisionInMeters(DistanceUnit.parse(fieldNode.toString(), DistanceUnit.DEFAULT, DistanceUnit.DEFAULT));
                    iterator.remove();
                } else if (Names.DISTANCE_ERROR_PCT.equals(fieldName)) {
                    builder.fieldType().setDistanceErrorPct(Double.parseDouble(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.ORIENTATION.equals(fieldName)) {
                    builder.fieldType().setOrientation(ShapeBuilder.Orientation.fromString(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.STRATEGY.equals(fieldName)) {
                    builder.fieldType().setStrategyName(fieldNode.toString());
                    iterator.remove();
                } else if (IGNORE_MALFORMED.equals(fieldName)) {
                    builder.ignoreMalformed(TypeParsers.nodeBooleanValue(fieldName, "ignore_malformed", fieldNode, parserContext));
                    iterator.remove();
                } else if (Names.COERCE.equals(fieldName)) {
                    builder.coerce(TypeParsers.nodeBooleanValue(fieldName, Names.COERCE, fieldNode, parserContext));
                    iterator.remove();
                } else if (Names.STRATEGY_POINTS_ONLY.equals(fieldName)
                    && builder.fieldType().strategyName.equals(SpatialStrategy.TERM.getStrategyName()) == false) {
                    boolean pointsOnly = TypeParsers.nodeBooleanValue(fieldName, Names.STRATEGY_POINTS_ONLY, fieldNode, parserContext);
                    builder.fieldType().setPointsOnly(pointsOnly);
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class GeoShapeFieldType extends MappedFieldType {

        private String tree = Defaults.TREE;
        private String strategyName = Defaults.STRATEGY;
        private boolean pointsOnly = Defaults.POINTS_ONLY;
        private int treeLevels = 0;
        private double precisionInMeters = -1;
        private Double distanceErrorPct;
        private double defaultDistanceErrorPct = 0.0;
        private Orientation orientation = Defaults.ORIENTATION;

        // these are built when the field type is frozen
        private PrefixTreeStrategy defaultStrategy;
        private RecursivePrefixTreeStrategy recursiveStrategy;
        private TermQueryPrefixTreeStrategy termStrategy;

        public GeoShapeFieldType() {}

        protected GeoShapeFieldType(GeoShapeFieldType ref) {
            super(ref);
            this.tree = ref.tree;
            this.strategyName = ref.strategyName;
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
                Objects.equals(strategyName, that.strategyName) &&
                pointsOnly == that.pointsOnly &&
                Objects.equals(distanceErrorPct, that.distanceErrorPct) &&
                orientation == that.orientation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), tree, strategyName, pointsOnly, treeLevels, precisionInMeters, distanceErrorPct,
                    defaultDistanceErrorPct, orientation);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void freeze() {
            super.freeze();
            // This is a bit hackish: we need to setup the spatial tree and strategies once the field name is set, which
            // must be by the time freeze is called.
            SpatialPrefixTree prefixTree;
            if ("geohash".equals(tree)) {
                prefixTree = new GeohashPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, getLevels(treeLevels, precisionInMeters, Defaults.GEOHASH_LEVELS, true));
            } else if ("legacyquadtree".equals(tree)) {
                prefixTree = new QuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, getLevels(treeLevels, precisionInMeters, Defaults.QUADTREE_LEVELS, false));
            } else if ("quadtree".equals(tree)) {
                prefixTree = new PackedQuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, getLevels(treeLevels, precisionInMeters, Defaults.QUADTREE_LEVELS, false));
            } else {
                throw new IllegalArgumentException("Unknown prefix tree type [" + tree + "]");
            }

            recursiveStrategy = new RecursivePrefixTreeStrategy(prefixTree, name());
            recursiveStrategy.setDistErrPct(distanceErrorPct());
            recursiveStrategy.setPruneLeafyBranches(false);
            termStrategy = new TermQueryPrefixTreeStrategy(prefixTree, name());
            termStrategy.setDistErrPct(distanceErrorPct());
            defaultStrategy = resolveStrategy(strategyName);
            defaultStrategy.setPointsOnly(pointsOnly);
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts, boolean strict) {
            super.checkCompatibility(fieldType, conflicts, strict);
            GeoShapeFieldType other = (GeoShapeFieldType)fieldType;
            // prevent user from changing strategies
            if (strategyName().equals(other.strategyName()) == false) {
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

            if (strict) {
                if (orientation() != other.orientation()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update [orientation] across all types.");
                }
                if (distanceErrorPct() != other.distanceErrorPct()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update [distance_error_pct] across all types.");
                }
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

        public String strategyName() {
            return strategyName;
        }

        public void setStrategyName(String strategyName) {
            checkIfFrozen();
            this.strategyName = strategyName;
            if (this.strategyName.equals(SpatialStrategy.TERM.getStrategyName())) {
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

        public PrefixTreeStrategy defaultStrategy() {
            return this.defaultStrategy;
        }

        public PrefixTreeStrategy resolveStrategy(SpatialStrategy strategy) {
            return resolveStrategy(strategy.getStrategyName());
        }

        public PrefixTreeStrategy resolveStrategy(String strategyName) {
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

    public GeoShapeFieldMapper(String simpleName, MappedFieldType fieldType, Explicit<Boolean> ignoreMalformed,
                               Explicit<Boolean> coerce, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, multiFields, copyTo);
        this.coerce = coerce;
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }
    @Override
    public Mapper parse(ParseContext context) throws IOException {
        try {
            Shape shape = context.parseExternalValue(Shape.class);
            if (shape == null) {
                ShapeBuilder shapeBuilder = ShapeBuilder.parse(context.parser(), this);
                if (shapeBuilder == null) {
                    return null;
                }
                shape = shapeBuilder.build();
            }
            if (fieldType().pointsOnly() == true) {
                // index configured for pointsOnly
                if (shape instanceof XShapeCollection && XShapeCollection.class.cast(shape).pointsOnly()) {
                    // MULTIPOINT data: index each point separately
                    List<Shape> shapes = ((XShapeCollection) shape).getShapes();
                    for (Shape s : shapes) {
                        indexShape(context, s);
                    }
                } else if (shape instanceof Point == false) {
                    throw new MapperParsingException("[{" + fieldType().name() + "}] is configured for points only but a " +
                        ((shape instanceof JtsGeometry) ? ((JtsGeometry)shape).getGeom().getGeometryType() : shape.getClass()) + " was found");
                }
            } else {
                indexShape(context, shape);
            }
        } catch (Exception e) {
            if (ignoreMalformed.value() == false) {
                throw new MapperParsingException("failed to parse [" + fieldType().name() + "]", e);
            }
        }
        return null;
    }

    private void indexShape(ParseContext context, Shape shape) {
        List<IndexableField> fields = new ArrayList<>(Arrays.asList(fieldType().defaultStrategy().createIndexableFields(shape)));
        createFieldNamesField(context, fields);
        for (IndexableField field : fields) {
            context.doc().add(field);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);

        GeoShapeFieldMapper gsfm = (GeoShapeFieldMapper)mergeWith;
        if (gsfm.coerce.explicit()) {
            this.coerce = gsfm.coerce;
        }
        if (gsfm.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gsfm.ignoreMalformed;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());

        if (includeDefaults || fieldType().tree().equals(Defaults.TREE) == false) {
            builder.field(Names.TREE, fieldType().tree());
        }
        if (includeDefaults || fieldType().treeLevels() != 0) {
            builder.field(Names.TREE_LEVELS, fieldType().treeLevels());
        }
        if (includeDefaults || fieldType().precisionInMeters() != -1) {
            builder.field(Names.TREE_PRESISION, DistanceUnit.METERS.toString(fieldType().precisionInMeters()));
        }
        if (includeDefaults || fieldType().strategyName() != Defaults.STRATEGY) {
            builder.field(Names.STRATEGY, fieldType().strategyName());
        }
        if (includeDefaults || fieldType().distanceErrorPct() != fieldType().defaultDistanceErrorPct) {
            builder.field(Names.DISTANCE_ERROR_PCT, fieldType().distanceErrorPct());
        }
        if (includeDefaults || fieldType().orientation() != Defaults.ORIENTATION) {
            builder.field(Names.ORIENTATION, fieldType().orientation());
        }
        if (includeDefaults || fieldType().pointsOnly() != GeoShapeFieldMapper.Defaults.POINTS_ONLY) {
            builder.field(Names.STRATEGY_POINTS_ONLY, fieldType().pointsOnly());
        }
        if (includeDefaults || coerce.explicit()) {
            builder.field(Names.COERCE, coerce.value());
        }
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(IGNORE_MALFORMED, ignoreMalformed.value());
        }
    }

    public Explicit<Boolean> coerce() {
        return coerce;
    }

    public Explicit<Boolean> ignoreMalformed() {
        return ignoreMalformed;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
