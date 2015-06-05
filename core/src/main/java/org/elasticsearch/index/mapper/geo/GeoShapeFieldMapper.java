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
package org.elasticsearch.index.mapper.geo;

import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.geoShapeField;


/**
 * FieldMapper for indexing {@link com.spatial4j.core.shape.Shape}s.
 * <p/>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.elasticsearch.index.query.GeoShapeFilterParser}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p/>
 * Format supported:
 * <p/>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 */
public class GeoShapeFieldMapper extends AbstractFieldMapper {

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
    }

    public static class Defaults {
        public static final String TREE = Names.TREE_GEOHASH;
        public static final String STRATEGY = SpatialStrategy.RECURSIVE.getStrategyName();
        public static final int GEOHASH_LEVELS = GeoUtils.geoHashLevelsForPrecision("50m");
        public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision("50m");
        public static final double DISTANCE_ERROR_PCT = 0.025d;
        public static final Orientation ORIENTATION = Orientation.RIGHT;

        public static final MappedFieldType FIELD_TYPE = new GeoShapeFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, GeoShapeFieldMapper> {

        private String tree = Defaults.TREE;
        private String strategyName = Defaults.STRATEGY;
        private int treeLevels = 0;
        private double precisionInMeters = -1;
        private double distanceErrorPct = Defaults.DISTANCE_ERROR_PCT;
        private boolean distErrPctDefined;
        private Orientation orientation = Defaults.ORIENTATION;

        private SpatialPrefixTree prefixTree;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
        }

        public Builder tree(String tree) {
            this.tree = tree;
            return this;
        }

        public Builder strategy(String strategy) {
            this.strategyName = strategy;
            return this;
        }

        public Builder treeLevelsByDistance(double meters) {
            this.precisionInMeters = meters;
            return this;
        }

        public Builder treeLevels(int treeLevels) {
            this.treeLevels = treeLevels;
            return this;
        }

        public Builder distanceErrorPct(double distanceErrorPct) {
            this.distanceErrorPct = distanceErrorPct;
            return this;
        }

        public Builder orientation(Orientation orientation) {
            this.orientation = orientation;
            return this;
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {

            if (Names.TREE_GEOHASH.equals(tree)) {
                prefixTree = new GeohashPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, getLevels(treeLevels, precisionInMeters, Defaults.GEOHASH_LEVELS, true));
            } else if (Names.TREE_QUADTREE.equals(tree)) {
                if (context.indexCreatedVersion().before(Version.V_1_6_0)) {
                    prefixTree = new QuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, getLevels(treeLevels, precisionInMeters, Defaults
                            .QUADTREE_LEVELS, false));
                } else {
                    prefixTree = new PackedQuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, getLevels(treeLevels, precisionInMeters, Defaults
                            .QUADTREE_LEVELS, false));
                }
            } else {
                throw new IllegalArgumentException("Unknown prefix tree type [" + tree + "]");
            }
            setupFieldType(context);

            RecursivePrefixTreeStrategy recursiveStrategy = new RecursivePrefixTreeStrategy(prefixTree, fieldType.names().indexName());
            recursiveStrategy.setDistErrPct(distanceErrorPct);
            recursiveStrategy.setPruneLeafyBranches(false);
            TermQueryPrefixTreeStrategy termStrategy = new TermQueryPrefixTreeStrategy(prefixTree, fieldType.names().indexName());
            termStrategy.setDistErrPct(distanceErrorPct);

            GeoShapeFieldType geoShapeFieldType = (GeoShapeFieldType)fieldType;
            geoShapeFieldType.setStrategies(strategyName, recursiveStrategy, termStrategy);
            geoShapeFieldType.setOrientation(orientation);

            return new GeoShapeFieldMapper(fieldType, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }

        private final int getLevels(int treeLevels, double precisionInMeters, int defaultLevels, boolean geoHash) {
            if (treeLevels > 0 || precisionInMeters >= 0) {
                // if the user specified a precision but not a distance error percent then zero out the distance err pct
                // this is done to guarantee precision specified by the user without doing something unexpected under the covers
                if (!distErrPctDefined) distanceErrorPct = 0;
                return Math.max(treeLevels, precisionInMeters >= 0 ? (geoHash ? GeoUtils.geoHashLevelsForPrecision(precisionInMeters)
                        : GeoUtils.quadTreeLevelsForPrecision(precisionInMeters)) : 0);
            }
            return defaultLevels;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = geoShapeField(name);
            // if index was created before 1.6, this conditional should be true (this forces any index created on/or after 1.6 to use 0 for
            // the default distanceErrorPct parameter).
            builder.distErrPctDefined = parserContext.indexVersionCreated().before(Version.V_1_6_0);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (Names.TREE.equals(fieldName)) {
                    builder.tree(fieldNode.toString());
                    iterator.remove();
                } else if (Names.TREE_LEVELS.equals(fieldName)) {
                    builder.treeLevels(Integer.parseInt(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.TREE_PRESISION.equals(fieldName)) {
                    builder.treeLevelsByDistance(DistanceUnit.parse(fieldNode.toString(), DistanceUnit.DEFAULT, DistanceUnit.DEFAULT));
                    iterator.remove();
                } else if (Names.DISTANCE_ERROR_PCT.equals(fieldName)) {
                    builder.distanceErrorPct(Double.parseDouble(fieldNode.toString()));
                    builder.distErrPctDefined = true;
                    iterator.remove();
                } else if (Names.ORIENTATION.equals(fieldName)) {
                    builder.orientation(ShapeBuilder.orientationFromString(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.STRATEGY.equals(fieldName)) {
                    builder.strategy(fieldNode.toString());
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class GeoShapeFieldType extends MappedFieldType {

        private PrefixTreeStrategy defaultStrategy;
        private RecursivePrefixTreeStrategy recursiveStrategy;
        private TermQueryPrefixTreeStrategy termStrategy;
        private Orientation orientation;

        public GeoShapeFieldType() {
            super(AbstractFieldMapper.Defaults.FIELD_TYPE);
        }

        protected GeoShapeFieldType(GeoShapeFieldType ref) {
            super(ref);
            // TODO: this shallow copy is probably not good...need to extract the parameters and recreate the tree and strategies?
            this.defaultStrategy = ref.defaultStrategy;
            this.recursiveStrategy = ref.recursiveStrategy;
            this.termStrategy = ref.termStrategy;
            this.orientation = ref.orientation;
        }

        @Override
        public MappedFieldType clone() {
            return new GeoShapeFieldType(this);
        }

        public PrefixTreeStrategy defaultStrategy() {
            return this.defaultStrategy;
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

        public void setStrategies(String defaultStrategy, RecursivePrefixTreeStrategy recursiveStrategy, TermQueryPrefixTreeStrategy termStrategy) {
            checkIfFrozen();
            this.recursiveStrategy = recursiveStrategy;
            this.termStrategy = termStrategy;
            this.defaultStrategy = resolveStrategy(defaultStrategy);
        }

        public void setDistErrPct(double distErrPct) {
            checkIfFrozen();
            this.recursiveStrategy.setDistErrPct(distErrPct);
            this.termStrategy.setDistErrPct(distErrPct);
        }

        public Orientation orientation() { return this.orientation; }

        public void setOrientation(Orientation orientation) {
            checkIfFrozen();
            this.orientation = orientation;
        }

        @Override
        public String value(Object value) {
            throw new UnsupportedOperationException("GeoShape fields cannot be converted to String values");
        }

    }

    public GeoShapeFieldMapper(MappedFieldType fieldType, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(fieldType, false, null, indexSettings, multiFields, copyTo);
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType)fieldType;
    }

    @Override
    public MappedFieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return null;
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
            Field[] fields = fieldType().defaultStrategy().createIndexableFields(shape);
            if (fields == null || fields.length == 0) {
                return null;
            }
            for (Field field : fields) {
                if (!customBoost()) {
                    field.setBoost(fieldType.boost());
                }
                context.doc().add(field);
            }
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse [" + fieldType.names().fullName() + "]", e);
        }
        return null;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        if (!this.getClass().equals(mergeWith.getClass())) {
            mergeResult.addConflict("mapper [" + fieldType.names().fullName() + "] has different field type");
            return;
        }
        final GeoShapeFieldMapper fieldMergeWith = (GeoShapeFieldMapper) mergeWith;
        final PrefixTreeStrategy mergeWithStrategy = fieldMergeWith.fieldType().defaultStrategy();

        // prevent user from changing strategies
        if (!(this.fieldType().defaultStrategy().getClass().equals(mergeWithStrategy.getClass()))) {
            mergeResult.addConflict("mapper [" + fieldType.names().fullName() + "] has different strategy");
        }

        final SpatialPrefixTree grid = this.fieldType().defaultStrategy().getGrid();
        final SpatialPrefixTree mergeGrid = mergeWithStrategy.getGrid();

        // prevent user from changing trees (changes encoding)
        if (!grid.getClass().equals(mergeGrid.getClass())) {
            mergeResult.addConflict("mapper [" + fieldType.names().fullName() + "] has different tree");
        }

        // TODO we should allow this, but at the moment levels is used to build bookkeeping variables
        // in lucene's SpatialPrefixTree implementations, need a patch to correct that first
        if (grid.getMaxLevels() != mergeGrid.getMaxLevels()) {
            mergeResult.addConflict("mapper [" + fieldType.names().fullName() + "] has different tree_levels or precision");
        }

        // bail if there were merge conflicts
        if (mergeResult.hasConflicts() || mergeResult.simulate()) {
            return;
        }

        // change distance error percent
        this.fieldType = this.fieldType.clone();
        this.fieldType().setDistErrPct(mergeWithStrategy.getDistErrPct());
        // change orientation - this is allowed because existing dateline spanning shapes
        // have already been unwound and segmented
        this.fieldType().setOrientation(fieldMergeWith.fieldType().orientation());
        this.fieldType.freeze();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());

        // TODO: Come up with a better way to get the name, maybe pass it from builder
        if (fieldType().defaultStrategy().getGrid() instanceof GeohashPrefixTree) {
            // Don't emit the tree name since GeohashPrefixTree is the default
            // Only emit the tree levels if it isn't the default value
            if (includeDefaults || fieldType().defaultStrategy().getGrid().getMaxLevels() != Defaults.GEOHASH_LEVELS) {
                builder.field(Names.TREE_LEVELS, fieldType().defaultStrategy().getGrid().getMaxLevels());
            }
        } else {
            builder.field(Names.TREE, Names.TREE_QUADTREE);
            if (includeDefaults || fieldType().defaultStrategy().getGrid().getMaxLevels() != Defaults.QUADTREE_LEVELS) {
                builder.field(Names.TREE_LEVELS, fieldType().defaultStrategy().getGrid().getMaxLevels());
            }
        }

        if (includeDefaults || fieldType().defaultStrategy().getDistErrPct() != Defaults.DISTANCE_ERROR_PCT) {
            builder.field(Names.DISTANCE_ERROR_PCT, fieldType().defaultStrategy().getDistErrPct());
        }

        if (includeDefaults || fieldType().orientation() != Defaults.ORIENTATION) {
            builder.field(Names.ORIENTATION, fieldType().orientation());
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
