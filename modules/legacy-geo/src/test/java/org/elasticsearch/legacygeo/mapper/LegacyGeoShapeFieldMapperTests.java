/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.legacygeo.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.legacygeo.test.TestLegacyGeoShapeFieldMapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
public class LegacyGeoShapeFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return "POINT (14.0 15.0)";
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_shape").field("strategy", "recursive");
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {

        checker.registerConflictCheck("strategy", fieldMapping(this::minimalMapping), fieldMapping(b -> {
            b.field("type", "geo_shape");
            b.field("strategy", "term");
        }));

        checker.registerConflictCheck("tree", b -> b.field("tree", "geohash"));
        checker.registerConflictCheck("tree_levels", b -> b.field("tree_levels", 5));
        checker.registerConflictCheck("precision", b -> b.field("precision", 10));
        checker.registerConflictCheck("points_only", b -> b.field("points_only", true));
        checker.registerUpdateCheck(b -> b.field("orientation", "right"), m -> {
            LegacyGeoShapeFieldMapper gsfm = (LegacyGeoShapeFieldMapper) m;
            assertEquals(Orientation.RIGHT, gsfm.orientation());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            LegacyGeoShapeFieldMapper gpfm = (LegacyGeoShapeFieldMapper) m;
            assertFalse(gpfm.ignoreZValue());
        });
        checker.registerUpdateCheck(b -> b.field("coerce", true), m -> {
            LegacyGeoShapeFieldMapper gpfm = (LegacyGeoShapeFieldMapper) m;
            assertTrue(gpfm.coerce());
        });
        // TODO - distance_error_pct ends up being subsumed into a calculated value, how to test
        checker.registerUpdateCheck(b -> b.field("distance_error_pct", 0.8), m -> {});
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new TestLegacyGeoShapeFieldMapperPlugin());
    }

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    @Override
    protected Version getVersion() {
        return VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
    }

    public void testLegacySwitches() throws IOException {
        // if one of the legacy parameters is added to a 'type':'geo_shape' config then
        // that will select the legacy field mapper
        testLegacySwitch("strategy", b -> b.field("strategy", "term"));
        testLegacySwitch("tree", b -> b.field("tree", "geohash"));
        testLegacySwitch("tree_levels", b -> b.field("tree_levels", 5));
        testLegacySwitch("precision", b -> b.field("precision", 10));
        testLegacySwitch("points_only", b -> b.field("points_only", true));
        testLegacySwitch("distance_error_pct", b -> b.field("distance_error_pct", 0.8));
    }

    private void testLegacySwitch(String field, CheckedConsumer<XContentBuilder, IOException> config) throws IOException {
        createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_shape");
            config.accept(b);
        }));
        Set<String> warnings = new HashSet<>();
        warnings.add(field);
        warnings.add("strategy");
        assertFieldWarnings(warnings.toArray(String[]::new));
    }

    public void testDefaultConfiguration() throws IOException {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().tree(), equalTo(LegacyGeoShapeFieldMapper.Defaults.TREE));
        assertThat(geoShapeFieldMapper.fieldType().treeLevels(), equalTo(0));
        assertThat(geoShapeFieldMapper.fieldType().pointsOnly(), equalTo(LegacyGeoShapeFieldMapper.Defaults.POINTS_ONLY));
        assertThat(geoShapeFieldMapper.fieldType().distanceErrorPct(), equalTo(LegacyGeoShapeFieldMapper.Defaults.DISTANCE_ERROR_PCT));
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(Orientation.RIGHT));
        assertFieldWarnings("strategy");
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("orientation", "left"))
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        Orientation orientation = ((LegacyGeoShapeFieldMapper) fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.LEFT));
        assertThat(orientation, equalTo(Orientation.CW));

        // explicit right orientation test
        mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("orientation", "right"))
        );
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        orientation = ((LegacyGeoShapeFieldMapper) fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.RIGHT));
        assertThat(orientation, equalTo(Orientation.CCW));
        assertFieldWarnings("tree", "strategy");
    }

    /**
     * Test that coerce parameter correctly parses
     */
    public void testCoerceParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("coerce", true))
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        boolean coerce = ((LegacyGeoShapeFieldMapper) fieldMapper).coerce();
        assertThat(coerce, equalTo(true));

        // explicit false coerce test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("coerce", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        coerce = ((LegacyGeoShapeFieldMapper) fieldMapper).coerce();
        assertThat(coerce, equalTo(false));
        assertFieldWarnings("tree", "strategy");
    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("ignore_z_value", true))
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        boolean ignoreZValue = ((LegacyGeoShapeFieldMapper) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("ignore_z_value", false))
        );
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        ignoreZValue = ((LegacyGeoShapeFieldMapper) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(false));
        assertFieldWarnings("strategy", "tree");
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of();
    }

    public void testGeohashConfiguration() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "geo_shape").field("tree", "geohash").field("tree_levels", "4").field("distance_error_pct", "0.1")
            )
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.1));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(4));
        assertFieldWarnings("tree", "tree_levels", "distance_error_pct", "strategy");
    }

    public void testQuadtreeConfiguration() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "geo_shape")
                    .field("tree", "quadtree")
                    .field("tree_levels", "6")
                    .field("distance_error_pct", "0.5")
                    .field("points_only", true)
            )
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.5));
        assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(6));
        assertThat(strategy.isPointsOnly(), equalTo(true));
        assertFieldWarnings("tree", "tree_levels", "distance_error_pct", "points_only", "strategy");
    }

    private void assertFieldWarnings(String... fieldNames) {
        String[] warnings = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            warnings[i] = "Parameter [" + fieldNames[i] + "] is deprecated and will be removed in a future version";
        }
        assertWarnings(warnings);
    }

    public void testLevelPrecisionConfiguration() throws IOException {
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("tree_levels", "6")
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                )
            );
            Mapper fieldMapper = mapper.mappers().getMapper("field");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            // 70m is more precise so it wins
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(70d)));
        }

        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "geo_shape").field("tree", "quadtree").field("tree_levels", "26").field("precision", "70m")
                )
            );
            Mapper fieldMapper = mapper.mappers().getMapper("field");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            // distance_error_pct was not specified so we expect the mapper to take the highest precision between "precision" and
            // "tree_levels" setting distErrPct to 0 to guarantee desired precision
            assertThat(strategy.getDistErrPct(), equalTo(0.0));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            // 70m is less precise so it loses
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(26));
        }

        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("tree_levels", "6")
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                )
            );
            Mapper fieldMapper = mapper.mappers().getMapper("field");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            // 70m is more precise so it wins
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(70d)));
        }

        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("tree_levels", GeoUtils.geoHashLevelsForPrecision(70d) + 1)
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                )
            );
            Mapper fieldMapper = mapper.mappers().getMapper("field");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(70d) + 1));
        }

        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("tree_levels", GeoUtils.quadTreeLevelsForPrecision(70d) + 1)
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                )
            );
            Mapper fieldMapper = mapper.mappers().getMapper("field");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(70d) + 1));
        }
        assertFieldWarnings("tree", "tree_levels", "precision", "distance_error_pct", "strategy");
    }

    public void testPointsOnlyOption() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "geohash").field("points_only", true))
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.isPointsOnly(), equalTo(true));
        assertFieldWarnings("tree", "points_only", "strategy");
    }

    public void testLevelDefaults() throws IOException {
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("distance_error_pct", "0.5"))
            );
            Mapper fieldMapper = mapper.mappers().getMapper("field");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            /* 50m is default */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(50d)));
        }

        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "geo_shape").field("tree", "geohash").field("distance_error_pct", "0.5"))
            );
            Mapper fieldMapper = mapper.mappers().getMapper("field");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            /* 50m is default */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(50d)));
        }
        assertFieldWarnings("tree", "distance_error_pct", "strategy");
    }

    public void testGeoShapeMapperMerge() throws Exception {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "geo_shape")
                    .field("tree", "geohash")
                    .field("strategy", "recursive")
                    .field("precision", "1m")
                    .field("tree_levels", 8)
                    .field("distance_error_pct", 0.01)
                    .field("orientation", "ccw")
            )
        );
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(Orientation.CCW));

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(
                mapperService,
                fieldMapping(
                    b -> b.field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("strategy", "term")
                        .field("precision", "1km")
                        .field("tree_levels", 26)
                        .field("distance_error_pct", 26)
                        .field("orientation", "cw")
                )
            )
        );
        assertThat(e.getMessage(), containsString("Cannot update parameter [strategy] from [recursive] to [term]"));
        assertThat(e.getMessage(), containsString("Cannot update parameter [tree] from [geohash] to [quadtree]"));
        assertThat(e.getMessage(), containsString("Cannot update parameter [tree_levels] from [8] to [26]"));
        assertThat(e.getMessage(), containsString("Cannot update parameter [precision] from [1.0m] to [1.0km]"));

        // verify nothing changed
        fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();
        assertThat(strategy, instanceOf(RecursivePrefixTreeStrategy.class));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getDistErrPct(), equalTo(0.01));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(1d)));
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(Orientation.CCW));

        // correct mapping
        merge(
            mapperService,
            fieldMapping(
                b -> b.field("type", "geo_shape")
                    .field("tree", "geohash")
                    .field("strategy", "recursive")
                    .field("precision", "1m")
                    .field("tree_levels", 8)
                    .field("distance_error_pct", 0.001)
                    .field("orientation", "cw")
            )
        );
        fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy, instanceOf(RecursivePrefixTreeStrategy.class));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getDistErrPct(), equalTo(0.001));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(1d)));
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(Orientation.CW));

        assertFieldWarnings("tree", "strategy", "precision", "tree_levels", "distance_error_pct");
    }

    public void testSerializeDefaults() throws Exception {
        ToXContent.Params includeDefaults = new ToXContent.MapParams(singletonMap("include_defaults", "true"));
        {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree")));
            String serialized = Strings.toString(mapper.mappers().getMapper("field"), includeDefaults);
            assertTrue(serialized, serialized.contains("\"precision\":\"50.0m\""));
            assertTrue(serialized, serialized.contains("\"tree_levels\":21"));
        }
        {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("tree", "geohash")));
            String serialized = Strings.toString(mapper.mappers().getMapper("field"), includeDefaults);
            assertTrue(serialized, serialized.contains("\"precision\":\"50.0m\""));
            assertTrue(serialized, serialized.contains("\"tree_levels\":9"));
        }
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("tree_levels", "6"))
            );
            String serialized = Strings.toString(mapper.mappers().getMapper("field"), includeDefaults);
            assertFalse(serialized, serialized.contains("\"precision\":"));
            assertTrue(serialized, serialized.contains("\"tree_levels\":6"));
        }
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("precision", "6"))
            );
            String serialized = Strings.toString(mapper.mappers().getMapper("field"), includeDefaults);
            assertTrue(serialized, serialized.contains("\"precision\":\"6.0m\""));
            assertFalse(serialized, serialized.contains("\"tree_levels\":"));
        }
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("precision", "6m").field("tree_levels", "5"))
            );
            String serialized = Strings.toString(mapper.mappers().getMapper("field"), includeDefaults);
            assertTrue(serialized, serialized.contains("\"precision\":\"6.0m\""));
            assertTrue(serialized, serialized.contains("\"tree_levels\":5"));
        }
        assertFieldWarnings("tree", "tree_levels", "precision", "strategy");
    }

    public void testPointsOnlyDefaultsWithTermStrategy() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("precision", "10m").field("strategy", "term"))
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.0));
        assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(23));
        assertThat(strategy.isPointsOnly(), equalTo(true));
        // term strategy changes the default for points_only, check that we handle it correctly
        assertThat(Strings.toString(geoShapeFieldMapper), not(containsString("points_only")));
        assertFieldWarnings("tree", "precision", "strategy");
    }

    public void testPointsOnlyFalseWithTermStrategy() throws Exception {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(
                    b -> b.field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("precision", "10m")
                        .field("strategy", "term")
                        .field("points_only", false)
                )
            )
        );
        assertThat(e.getMessage(), containsString("points_only cannot be set to false for term strategy"));
        assertFieldWarnings("tree", "precision", "strategy", "points_only");
    }

    public void testDisallowExpensiveQueries() throws IOException {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.allowExpensiveQueries()).thenReturn(false);
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree")));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> geoShapeFieldMapper.fieldType()
                .geoShapeQuery(searchExecutionContext, "location", SpatialStrategy.TERM, ShapeRelation.INTERSECTS, new Point(-10, 10))
        );
        assertEquals(
            "[geo-shape] queries on [PrefixTree geo shapes] cannot be executed when " + "'search.allow_expensive_queries' is set to false.",
            e.getMessage()
        );
        assertFieldWarnings("tree", "strategy");
    }

    @Override
    protected String[] getParseMinimalWarnings() {
        return new String[] { "Parameter [strategy] is deprecated and will be removed in a future version" };
    }

    @Override
    protected String[] getParseMaximalWarnings() {
        return new String[] {
            "Parameter [strategy] is deprecated and will be removed in a future version",
            "Parameter [tree] is deprecated and will be removed in a future version",
            "Parameter [tree_levels] is deprecated and will be removed in a future version",
            "Parameter [precision] is deprecated and will be removed in a future version",
            "Parameter [distance_error_pct] is deprecated and will be removed in a future version",
            "Parameter [points_only] is deprecated and will be removed in a future version" };
    }

    public void testGeoShapeArrayParsing() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree")));
        ParsedDocument document = mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("type", "Point").startArray("coordinates").value(176.0).value(15.0).endArray().endObject();
                b.startObject().field("type", "Point").startArray("coordinates").value(76.0).value(-15.0).endArray().endObject();
            }
            b.endArray();
        }));
        assertThat(document.docs(), hasSize(1));
        List<IndexableField> fields = document.docs().get(0).getFields("field");
        assertThat(fields.size(), equalTo(2));
        assertFieldWarnings("tree", "strategy");
    }

    protected void assertSearchable(MappedFieldType fieldType) {
        // always searchable even if it uses TextSearchInfo.NONE
        assertTrue(fieldType.isSearchable());
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
