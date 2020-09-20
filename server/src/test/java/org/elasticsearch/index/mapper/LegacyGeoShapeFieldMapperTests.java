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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LegacyGeoShapeFieldMapperTests extends FieldMapperTestCase2<LegacyGeoShapeFieldMapper.Builder> {

    @Override
    protected LegacyGeoShapeFieldMapper.Builder newBuilder() {
        return new LegacyGeoShapeFieldMapper.Builder("geoshape");
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "doc_values", "store");
    }

    @Before
    public void addModifiers() {
        addModifier("tree", false, (a, b) -> {
            a.deprecatedParameters.tree = "geohash";
            b.deprecatedParameters.tree = "quadtree";
        });
        addModifier("strategy", false, (a, b) -> {
            a.deprecatedParameters.strategy = SpatialStrategy.TERM;
            b.deprecatedParameters.strategy = SpatialStrategy.RECURSIVE;
        });
        addModifier("tree_levels", false, (a, b) -> {
            a.deprecatedParameters.treeLevels = 2;
            b.deprecatedParameters.treeLevels = 3;
        });
        addModifier("precision", false, (a, b) -> {
            a.deprecatedParameters.precision = "10";
            b.deprecatedParameters.precision = "20";
        });
        addModifier("distance_error_pct", true, (a, b) -> {
            a.deprecatedParameters.distanceErrorPct = 0.5;
            b.deprecatedParameters.distanceErrorPct = 0.6;
        });
        addModifier("orientation", true, (a, b) -> {
            a.orientation = ShapeBuilder.Orientation.RIGHT;
            b.orientation = ShapeBuilder.Orientation.LEFT;
        });
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new TestGeoShapeFieldMapperPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_shape").field("strategy", "recursive");
    }

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    public void testDefaultConfiguration() throws IOException {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().tree(),
            equalTo(LegacyGeoShapeFieldMapper.DeprecatedParameters.Defaults.TREE));
        assertThat(geoShapeFieldMapper.fieldType().treeLevels(), equalTo(0));
        assertThat(geoShapeFieldMapper.fieldType().pointsOnly(),
            equalTo(LegacyGeoShapeFieldMapper.DeprecatedParameters.Defaults.POINTS_ONLY));
        assertThat(geoShapeFieldMapper.fieldType().distanceErrorPct(),
            equalTo(LegacyGeoShapeFieldMapper.DeprecatedParameters.Defaults.DISTANCE_ERROR_PCT));
        assertThat(geoShapeFieldMapper.fieldType().orientation(),
            equalTo(LegacyGeoShapeFieldMapper.Defaults.ORIENTATION.value()));
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
        ShapeBuilder.Orientation orientation = ((LegacyGeoShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // explicit right orientation test
        mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("orientation", "right"))
        );
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        orientation = ((LegacyGeoShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
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
        boolean coerce = ((LegacyGeoShapeFieldMapper)fieldMapper).coerce().value();
        assertThat(coerce, equalTo(true));

        // explicit false coerce test
        mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("coerce", false))
        );
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        coerce = ((LegacyGeoShapeFieldMapper)fieldMapper).coerce().value();
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
        boolean ignoreZValue = ((LegacyGeoShapeFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("ignore_z_value", false))
        );
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        ignoreZValue = ((LegacyGeoShapeFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(false));
        assertFieldWarnings("strategy", "tree");
    }

    /**
     * Test that ignore_malformed parameter correctly parses
     */
    public void testIgnoreMalformedParsing() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("ignore_malformed", true))
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        Explicit<Boolean> ignoreMalformed = ((LegacyGeoShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.value(), equalTo(true));

        // explicit false ignore_malformed test
        mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree").field("ignore_malformed", false))
        );
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        ignoreMalformed = ((LegacyGeoShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.explicit(), equalTo(true));
        assertThat(ignoreMalformed.value(), equalTo(false));
        assertFieldWarnings("tree", "strategy");
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
            warnings[i] = "Field parameter [" + fieldNames[i] + "] "
                + "is deprecated and will be removed in a future version.";
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
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CCW));

        Exception e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, fieldMapping(b -> b.field("type", "geo_shape")
            .field("tree", "quadtree")
            .field("strategy", "term").field("precision", "1km")
            .field("tree_levels", 26).field("distance_error_pct", 26)
            .field("orientation", "cw")))); 
        assertThat(e.getMessage(), containsString("mapper [field] has different [strategy]"));
        assertThat(e.getMessage(), containsString("mapper [field] has different [tree]"));
        assertThat(e.getMessage(), containsString("mapper [field] has different [tree_levels]"));
        assertThat(e.getMessage(), containsString("mapper [field] has different [precision]"));

        // verify nothing changed
        fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();
        assertThat(strategy, instanceOf(RecursivePrefixTreeStrategy.class));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getDistErrPct(), equalTo(0.01));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(1d)));
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CCW));

        // correct mapping
        merge(mapperService, fieldMapping(b -> b.field("type", "geo_shape")
            .field("tree", "geohash")
            .field("strategy", "recursive")
            .field("precision", "1m")
            .field("tree_levels", 8).field("distance_error_pct", 0.001)
            .field("orientation", "cw")));
        fieldMapper = mapperService.documentMapper().mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy, instanceOf(RecursivePrefixTreeStrategy.class));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getDistErrPct(), equalTo(0.001));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(1d)));
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CW));

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
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("tree", "quadtree")
            .field("precision", "10m")
            .field("strategy", "term")
            .endObject().endObject()
            .endObject().endObject());

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
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.allowExpensiveQueries()).thenReturn(false);
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_shape").field("tree", "quadtree")));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;

        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> geoShapeFieldMapper.fieldType().geometryQueryBuilder().process(
                        new Point(-10, 10), "location", SpatialStrategy.TERM, ShapeRelation.INTERSECTS, queryShardContext));
        assertEquals("[geo-shape] queries on [PrefixTree geo shapes] cannot be executed when " +
                        "'search.allow_expensive_queries' is set to false.", e.getMessage());
        assertFieldWarnings("tree", "strategy");
    }

    @Override
    protected void assertParseMinimalWarnings() {
        assertWarnings("Field parameter [strategy] is deprecated and will be removed in a future version.");
    }

    @Override
    protected void assertSerializationWarnings() {
        assertWarnings("Field parameter [strategy] is deprecated and will be removed in a future version.",
            "Field parameter [tree] is deprecated and will be removed in a future version.",
            "Field parameter [tree_levels] is deprecated and will be removed in a future version.",
            "Field parameter [precision] is deprecated and will be removed in a future version.",
            "Field parameter [distance_error_pct] is deprecated and will be removed in a future version."
            );
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
        IndexableField[] fields = document.docs().get(0).getFields("field");
        assertThat(fields.length, equalTo(2));
        assertFieldWarnings("tree", "strategy");
    }

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        LegacyGeoShapeFieldMapper mapper = new LegacyGeoShapeFieldMapper.Builder("field").build(context);
        SourceLookup sourceLookup = new SourceLookup();

        Map<String, Object> jsonLineString = Map.of("type", "LineString", "coordinates",
            List.of(List.of(42.0, 27.1), List.of(30.0, 50.0)));
        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(14.0, 15.0));
        String wktLineString = "LINESTRING (42.0 27.1, 30.0 50.0)";
        String wktPoint = "POINT (14.0 15.0)";

        // Test a single shape in geojson format.
        Object sourceValue = jsonLineString;
        assertEquals(List.of(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in geojson format.
        sourceValue = List.of(jsonLineString, jsonPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single shape in wkt format.
        sourceValue = wktLineString;
        assertEquals(List.of(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in wkt format.
        sourceValue = List.of(wktLineString, wktPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }
}
