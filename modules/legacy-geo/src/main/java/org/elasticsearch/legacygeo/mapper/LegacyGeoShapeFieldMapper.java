/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.legacygeo.mapper;

import org.apache.lucene.search.Query;
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
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.legacygeo.ShapesAvailability;
import org.elasticsearch.legacygeo.XShapeCollection;
import org.elasticsearch.legacygeo.builders.ShapeBuilder;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.legacygeo.query.LegacyGeoShapeQueryProcessor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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
 * @deprecated use {@link org.elasticsearch.index.mapper.GeoShapeFieldMapper}
 */
@Deprecated
public class LegacyGeoShapeFieldMapper extends AbstractShapeGeometryFieldMapper<ShapeBuilder<?, ?, ?>> {

    public static final String CONTENT_TYPE = "geo_shape";

    public static final Set<String> DEPRECATED_PARAMETERS = Set.of(
        "strategy",
        "tree",
        "tree_levels",
        "precision",
        "distance_error_pct",
        "points_only"
    );

    public static boolean containsDeprecatedParameter(Set<String> paramKeys) {
        return DEPRECATED_PARAMETERS.stream().anyMatch(paramKeys::contains);
    }

    public static Set<String> getDeprecatedParameters(Set<String> paramKeys) {
        return DEPRECATED_PARAMETERS.stream().filter((p) -> paramKeys.contains(p)).collect(Collectors.toSet());
    }

    public static class Defaults {
        public static final SpatialStrategy STRATEGY = SpatialStrategy.RECURSIVE;
        public static final String TREE = "quadtree";
        public static final String PRECISION = "50m";
        public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision(PRECISION);
        public static final int GEOHASH_TREE_LEVELS = GeoUtils.geoHashLevelsForPrecision(PRECISION);
        public static final boolean POINTS_ONLY = false;
        public static final double DISTANCE_ERROR_PCT = 0.025d;

        public static int defaultTreeLevel(String tree) {
            return switch (tree) {
                case PrefixTrees.GEOHASH -> GEOHASH_TREE_LEVELS;
                case PrefixTrees.LEGACY_QUADTREE, PrefixTrees.QUADTREE -> QUADTREE_LEVELS;
                default -> throw new IllegalArgumentException("Unknown prefix type [" + tree + "]");
            };
        }
    }

    public static class PrefixTrees {
        public static final String LEGACY_QUADTREE = "legacyquadtree";
        public static final String QUADTREE = "quadtree";
        public static final String GEOHASH = "geohash";
    }

    @Deprecated
    public static class DeprecatedParameters {

        private static void checkPrefixTreeSupport(String fieldName) {
            if (ShapesAvailability.JTS_AVAILABLE == false || ShapesAvailability.SPATIAL4J_AVAILABLE == false) {
                throw new ElasticsearchParseException("Field parameter [{}] is not supported for [{}] field type", fieldName, CONTENT_TYPE);
            }

        }
    }

    private static Builder builder(FieldMapper in) {
        return ((LegacyGeoShapeFieldMapper) in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<Explicit<Boolean>> coerce;
        Parameter<Explicit<Orientation>> orientation = orientationParam(m -> builder(m).orientation.get());

        Parameter<SpatialStrategy> strategy = new Parameter<>(
            "strategy",
            false,
            () -> SpatialStrategy.RECURSIVE,
            (n, c, o) -> SpatialStrategy.fromString(o.toString()),
            m -> builder(m).strategy.get(),
            (b, f, v) -> b.field(f, v.getStrategyName()),
            SpatialStrategy::getStrategyName
        ).deprecated();
        Parameter<String> tree = Parameter.stringParam("tree", false, m -> builder(m).tree.get(), Defaults.TREE).deprecated();
        Parameter<Integer> treeLevels = new Parameter<>(
            "tree_levels",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : XContentMapValues.nodeIntegerValue(o),
            m -> builder(m).treeLevels.get(),
            (b, f, v) -> {
                if (v != null && v != 0) {
                    b.field(f, v);
                } else {
                    b.field(f, Defaults.defaultTreeLevel(tree.get()));
                }
            },
            Objects::toString
        ).deprecated();
        Parameter<DistanceUnit.Distance> precision = new Parameter<>(
            "precision",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : DistanceUnit.Distance.parseDistance(o.toString()),
            m -> builder(m).precision.get(),
            (b, f, v) -> {
                if (v == null) {
                    b.field(f, "50.0m");
                } else {
                    b.field(f, v.toString());
                }
            },
            Objects::toString
        ).deprecated();
        Parameter<Double> distanceErrorPct = new Parameter<>(
            "distance_error_pct",
            true,
            () -> null,
            (n, c, o) -> o == null ? null : XContentMapValues.nodeDoubleValue(o),
            m -> builder(m).distanceErrorPct.get(),
            XContentBuilder::field,
            Objects::toString
        ).deprecated().acceptsNull();
        Parameter<Boolean> pointsOnly = new Parameter<>(
            "points_only",
            false,
            () -> null,
            (n, c, o) -> XContentMapValues.nodeBooleanValue(o),
            m -> builder(m).pointsOnly.get(),
            (b, f, v) -> {
                if (v == null) {
                    b.field(f, strategy.get() == SpatialStrategy.TERM);
                } else {
                    b.field(f, v);
                }
            },
            Objects::toString
        ).deprecated().acceptsNull();

        Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final Version indexCreatedVersion;

        public Builder(String name, Version version, boolean ignoreMalformedByDefault, boolean coerceByDefault) {
            super(name);

            if (ShapesAvailability.JTS_AVAILABLE == false || ShapesAvailability.SPATIAL4J_AVAILABLE == false) {
                throw new ElasticsearchParseException("Non-BKD field parameters are not supported for [{}] field type", CONTENT_TYPE);
            }

            this.indexCreatedVersion = version;
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.coerce = coerceParam(m -> builder(m).coerce.get(), coerceByDefault);

            this.pointsOnly.addValidator(v -> {
                if (v == null) {
                    return;
                }
                if (v == false && SpatialStrategy.TERM == strategy.get()) {
                    throw new IllegalArgumentException("points_only cannot be set to false for term strategy");
                }
            });

            // Set up serialization
            if (version.onOrAfter(Version.V_7_0_0)) {
                this.strategy.alwaysSerialize();
            }
            // serialize treeLevels if treeLevels is configured, OR if defaults are requested and precision is not configured
            treeLevels.setSerializerCheck((id, ic, v) -> ic || (id && precision.get() == null));
            // serialize precision if precision is configured, OR if defaults are requested and treeLevels is not configured
            precision.setSerializerCheck((id, ic, v) -> ic || (id && treeLevels.get() == null));
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(
                indexed,
                ignoreMalformed,
                ignoreZValue,
                coerce,
                orientation,
                strategy,
                tree,
                treeLevels,
                precision,
                distanceErrorPct,
                pointsOnly,
                meta
            );
        }

        public Builder coerce(boolean coerce) {
            this.coerce.setValue(Explicit.explicitBoolean(coerce));
            return this;
        }

        private void setupFieldTypeDeprecatedParameters(GeoShapeFieldType ft) {
            ft.setStrategy(strategy.get());
            ft.setTree(tree.get());
            if (treeLevels.get() != null) {
                ft.setTreeLevels(treeLevels.get());
            }
            if (precision.get() != null) {
                ft.setPrecisionInMeters(precision.get().value);
            }
            if (pointsOnly.get() != null) {
                ft.setPointsOnly(pointsOnly.get());
            }
            if (distanceErrorPct.get() != null) {
                ft.setDistanceErrorPct(distanceErrorPct.get());
            }
            if (ft.treeLevels() == 0 && ft.precisionInMeters() < 0) {
                ft.setDefaultDistanceErrorPct(Defaults.DISTANCE_ERROR_PCT);
            }
        }

        private void setupPrefixTrees(GeoShapeFieldType ft) {
            SpatialPrefixTree prefixTree;
            if (ft.tree().equals(PrefixTrees.GEOHASH)) {
                prefixTree = new GeohashPrefixTree(
                    ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(ft.treeLevels(), ft.precisionInMeters(), Defaults.GEOHASH_TREE_LEVELS, true)
                );
            } else if (ft.tree().equals(PrefixTrees.LEGACY_QUADTREE)) {
                prefixTree = new QuadPrefixTree(
                    ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(ft.treeLevels(), ft.precisionInMeters(), Defaults.QUADTREE_LEVELS, false)
                );
            } else if (ft.tree().equals(PrefixTrees.QUADTREE)) {
                prefixTree = new PackedQuadPrefixTree(
                    ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(ft.treeLevels(), ft.precisionInMeters(), Defaults.QUADTREE_LEVELS, false)
                );
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

        private GeoShapeFieldType buildFieldType(LegacyGeoShapeParser parser, MapperBuilderContext context) {
            GeoShapeFieldType ft = new GeoShapeFieldType(
                context.buildFullName(name),
                indexed.get(),
                orientation.get().value(),
                parser,
                meta.get()
            );
            setupFieldTypeDeprecatedParameters(ft);
            setupPrefixTrees(ft);
            return ft;
        }

        private static int getLevels(int treeLevels, double precisionInMeters, int defaultLevels, boolean geoHash) {
            if (treeLevels > 0 || precisionInMeters >= 0) {
                return Math.max(
                    treeLevels,
                    precisionInMeters >= 0
                        ? (geoHash
                            ? GeoUtils.geoHashLevelsForPrecision(precisionInMeters)
                            : GeoUtils.quadTreeLevelsForPrecision(precisionInMeters))
                        : 0
                );
            }
            return defaultLevels;
        }

        @Override
        public LegacyGeoShapeFieldMapper build(MapperBuilderContext context) {
            if (name.isEmpty()) {
                // Check for an empty name early so we can throw a consistent error message
                throw new IllegalArgumentException("name cannot be empty string");
            }
            LegacyGeoShapeParser parser = new LegacyGeoShapeParser();
            GeoShapeFieldType ft = buildFieldType(parser, context);
            return new LegacyGeoShapeFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), parser, this);
        }
    }

    @Deprecated
    public static Mapper.TypeParser PARSER = (name, node, parserContext) -> {
        boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(parserContext.getSettings());
        boolean coerceByDefault = COERCE_SETTING.get(parserContext.getSettings());
        FieldMapper.Builder builder = new LegacyGeoShapeFieldMapper.Builder(
            name,
            parserContext.indexVersionCreated(),
            ignoreMalformedByDefault,
            coerceByDefault
        );
        builder.parse(name, parserContext, node);
        return builder;
    };

    private static class LegacyGeoShapeParser extends Parser<ShapeBuilder<?, ?, ?>> {

        private LegacyGeoShapeParser() {}

        @Override
        public void parse(
            XContentParser parser,
            CheckedConsumer<ShapeBuilder<?, ?, ?>, IOException> consumer,
            Consumer<Exception> onMalformed
        ) throws IOException {
            try {
                if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        parse(parser, consumer, onMalformed);
                    }
                } else {
                    consumer.accept(ShapeParser.parse(parser));
                }
            } catch (ElasticsearchParseException e) {
                onMalformed.accept(e);
            }
        }

        @Override
        public ShapeBuilder<?, ?, ?> normalizeFromSource(ShapeBuilder<?, ?, ?> geometry) {
            return geometry;
        }
    }

    public static final class GeoShapeFieldType extends AbstractShapeGeometryFieldType<ShapeBuilder<?, ?, ?>> implements GeoShapeQueryable {

        private String tree = Defaults.TREE;
        private SpatialStrategy strategy = Defaults.STRATEGY;
        private boolean pointsOnly = Defaults.POINTS_ONLY;
        private int treeLevels = 0;
        private double precisionInMeters = -1;
        private Double distanceErrorPct;
        private double defaultDistanceErrorPct = 0.0;

        // these are built when the field type is frozen
        private PrefixTreeStrategy defaultPrefixTreeStrategy;
        private RecursivePrefixTreeStrategy recursiveStrategy;
        private TermQueryPrefixTreeStrategy termStrategy;

        private final LegacyGeoShapeQueryProcessor queryProcessor;

        private GeoShapeFieldType(
            String name,
            boolean indexed,
            Orientation orientation,
            LegacyGeoShapeParser parser,
            Map<String, String> meta
        ) {
            super(name, indexed, false, false, parser, orientation, meta);
            this.queryProcessor = new LegacyGeoShapeQueryProcessor(this);
        }

        public GeoShapeFieldType(String name) {
            this(name, true, Orientation.RIGHT, null, Collections.emptyMap());
        }

        @Override
        public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
            throw new UnsupportedOperationException("process method should not be called for PrefixTree based geo_shapes");
        }

        @Override
        public Query geoShapeQuery(
            Geometry shape,
            String fieldName,
            SpatialStrategy spatialStrategy,
            ShapeRelation relation,
            SearchExecutionContext context
        ) {
            return queryProcessor.geoShapeQuery(shape, fieldName, spatialStrategy, relation, context);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public String tree() {
            return tree;
        }

        public void setTree(String tree) {
            this.tree = tree;
        }

        public SpatialStrategy strategy() {
            return strategy;
        }

        public void setStrategy(SpatialStrategy strategy) {
            this.strategy = strategy;
            if (this.strategy.equals(SpatialStrategy.TERM)) {
                this.pointsOnly = true;
            }
        }

        public boolean pointsOnly() {
            return pointsOnly;
        }

        public void setPointsOnly(boolean pointsOnly) {
            this.pointsOnly = pointsOnly;
        }

        public int treeLevels() {
            return treeLevels;
        }

        public void setTreeLevels(int treeLevels) {
            this.treeLevels = treeLevels;
        }

        public double precisionInMeters() {
            return precisionInMeters;
        }

        public void setPrecisionInMeters(double precisionInMeters) {
            this.precisionInMeters = precisionInMeters;
        }

        public double distanceErrorPct() {
            return distanceErrorPct == null ? defaultDistanceErrorPct : distanceErrorPct;
        }

        public void setDistanceErrorPct(double distanceErrorPct) {
            this.distanceErrorPct = distanceErrorPct;
        }

        public void setDefaultDistanceErrorPct(double defaultDistanceErrorPct) {
            this.defaultDistanceErrorPct = defaultDistanceErrorPct;
        }

        public PrefixTreeStrategy defaultPrefixTreeStrategy() {
            return this.defaultPrefixTreeStrategy;
        }

        public PrefixTreeStrategy resolvePrefixTreeStrategy(SpatialStrategy spatialStrategy) {
            return resolvePrefixTreeStrategy(spatialStrategy.getStrategyName());
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
        protected Function<List<ShapeBuilder<?, ?, ?>>, List<Object>> getFormatter(String format) {
            return GeometryFormatterFactory.getFormatter(format, ShapeBuilder::buildGeometry);
        }
    }

    private final Version indexCreatedVersion;
    private final Builder builder;

    public LegacyGeoShapeFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        LegacyGeoShapeParser parser,
        Builder builder
    ) {
        super(
            simpleName,
            mappedFieldType,
            Collections.singletonMap(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER),
            builder.ignoreMalformed.get(),
            builder.coerce.get(),
            builder.ignoreZValue.get(),
            builder.orientation.get(),
            multiFields,
            copyTo,
            parser
        );
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.builder = builder;
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    public String strategy() {
        return fieldType().strategy().getStrategyName();
    }

    @Override
    protected void index(DocumentParserContext context, ShapeBuilder<?, ?, ?> shapeBuilder) throws IOException {
        if (shapeBuilder == null) {
            return;
        }
        Shape shape = shapeBuilder.buildS4J();
        if (fieldType().pointsOnly()) {
            // index configured for pointsOnly
            if (shape instanceof XShapeCollection && ((XShapeCollection<?>) shape).pointsOnly()) {
                // MULTIPOINT data: index each point separately
                @SuppressWarnings("unchecked")
                List<Shape> shapes = ((XShapeCollection<Shape>) shape).getShapes();
                for (Shape s : shapes) {
                    context.doc().addAll(Arrays.asList(fieldType().defaultPrefixTreeStrategy().createIndexableFields(s)));
                }
                return;
            } else if (shape instanceof Point == false) {
                throw new MapperParsingException(
                    "[{"
                        + fieldType().name()
                        + "}] is configured for points only but a "
                        + ((shape instanceof JtsGeometry) ? ((JtsGeometry) shape).getGeom().getGeometryType() : shape.getClass())
                        + " was found"
                );
            }
        }
        context.doc().addAll(Arrays.asList(fieldType().defaultPrefixTreeStrategy().createIndexableFields(shape)));
        context.addToFieldNames(fieldType().name());
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(
            simpleName(),
            indexCreatedVersion,
            builder.ignoreMalformed.getDefaultValue().value(),
            builder.coerce.getDefaultValue().value()
        ).init(this);
    }

    @Override
    protected void checkIncomingMergeType(FieldMapper mergeWith) {
        if (mergeWith instanceof LegacyGeoShapeFieldMapper == false && CONTENT_TYPE.equals(mergeWith.typeName())) {
            throw new IllegalArgumentException(
                "mapper [" + name() + "] of type [geo_shape] cannot change strategy from [recursive] to [BKD]"
            );
        }
        super.checkIncomingMergeType(mergeWith);
    }
}
