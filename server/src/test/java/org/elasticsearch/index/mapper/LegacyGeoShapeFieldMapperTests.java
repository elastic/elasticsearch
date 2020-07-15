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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.mapper.AbstractGeometryFieldMapper.Names.IGNORE_Z_VALUE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LegacyGeoShapeFieldMapperTests extends FieldMapperTestCase<LegacyGeoShapeFieldMapper.Builder> {

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
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    public void testDefaultConfiguration() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("strategy", "recursive")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        assertEquals(mapping, defaultMapper.mappingSource().toString());

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
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("orientation", "left")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        ShapeBuilder.Orientation orientation = ((LegacyGeoShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // explicit right orientation test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("orientation", "right")
                .endObject().endObject()
                .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        orientation = ((LegacyGeoShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
        assertFieldWarnings("tree");
    }

    /**
     * Test that coerce parameter correctly parses
     */
    public void testCoerceParsing() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("coerce", "true")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        boolean coerce = ((LegacyGeoShapeFieldMapper)fieldMapper).coerce().value();
        assertThat(coerce, equalTo(true));

        // explicit false coerce test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("coerce", "false")
                .endObject().endObject()
                .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        coerce = ((LegacyGeoShapeFieldMapper)fieldMapper).coerce().value();
        assertThat(coerce, equalTo(false));
        assertFieldWarnings("tree");
    }


    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("strategy", "recursive")
            .field(IGNORE_Z_VALUE.getPreferredName(), "true")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        boolean ignoreZValue = ((LegacyGeoShapeFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("tree", "quadtree")
            .field(IGNORE_Z_VALUE.getPreferredName(), "false")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        ignoreZValue = ((LegacyGeoShapeFieldMapper)fieldMapper).ignoreZValue().value();
        assertThat(ignoreZValue, equalTo(false));
        assertFieldWarnings("strategy", "tree");
    }

    /**
     * Test that ignore_malformed parameter correctly parses
     */
    public void testIgnoreMalformedParsing() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("tree", "quadtree")
            .field("ignore_malformed", "true")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        Explicit<Boolean> ignoreMalformed = ((LegacyGeoShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.value(), equalTo(true));

        // explicit false ignore_malformed test
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("tree", "quadtree")
            .field("ignore_malformed", "false")
            .endObject().endObject()
            .endObject().endObject());

        defaultMapper = createIndex("test2").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        ignoreMalformed = ((LegacyGeoShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.explicit(), equalTo(true));
        assertThat(ignoreMalformed.value(), equalTo(false));
        assertFieldWarnings("tree");
    }

    public void testGeohashConfiguration() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "geohash")
                    .field("tree_levels", "4")
                    .field("distance_error_pct", "0.1")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.1));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(4));
        assertFieldWarnings("tree", "tree_levels", "distance_error_pct");
    }

    public void testQuadtreeConfiguration() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "quadtree")
                    .field("tree_levels", "6")
                    .field("distance_error_pct", "0.5")
                    .field("points_only", true)
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.5));
        assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(6));
        assertThat(strategy.isPointsOnly(), equalTo(true));
        assertFieldWarnings("tree", "tree_levels", "distance_error_pct", "points_only");
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
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("tree_levels", "6")
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject());


            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            // 70m is more precise so it wins
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(70d)));
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "quadtree")
                    .field("tree_levels", "26")
                    .field("precision", "70m")
                    .endObject().endObject()
                    .endObject().endObject());


            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
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
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("tree_levels", "6")
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject());

            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            // 70m is more precise so it wins
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(70d)));
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("tree_levels",  GeoUtils.geoHashLevelsForPrecision(70d)+1)
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject());

            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            assertThat(strategy.getGrid().getMaxLevels(),  equalTo(GeoUtils.geoHashLevelsForPrecision(70d)+1));
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("tree_levels", GeoUtils.quadTreeLevelsForPrecision(70d)+1)
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject());

            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(70d)+1));
        }
        assertFieldWarnings("tree", "tree_levels", "precision", "distance_error_pct");
    }

    public void testPointsOnlyOption() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "geohash")
                .field("points_only", true)
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.isPointsOnly(), equalTo(true));
        assertFieldWarnings("tree", "points_only");
    }

    public void testLevelDefaults() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject());


            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            /* 50m is default */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(50d)));
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject());

            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            /* 50m is default */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(50d)));
        }
        assertFieldWarnings("tree", "distance_error_pct");
    }

    public void testGeoShapeMapperMerge() throws Exception {
        String stage1Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("shape").field("type", "geo_shape").field("tree", "geohash")
                .field("strategy", "recursive")
                .field("precision", "1m").field("tree_levels", 8).field("distance_error_pct", 0.01)
                .field("orientation", "ccw")
                .endObject().endObject().endObject().endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper docMapper = mapperService.merge("type", new CompressedXContent(stage1Mapping),
            MapperService.MergeReason.MAPPING_UPDATE);

        Mapper fieldMapper = docMapper.mappers().getMapper("shape");
        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CCW));

        String stage2Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("shape").field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("strategy", "term").field("precision", "1km")
                .field("tree_levels", 26).field("distance_error_pct", 26)
                .field("orientation", "cw").endObject().endObject().endObject().endObject());
        try {
            mapperService.merge("type", new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [shape] has different [strategy]"));
            assertThat(e.getMessage(), containsString("mapper [shape] has different [tree]"));
            assertThat(e.getMessage(), containsString("mapper [shape] has different [tree_levels]"));
            assertThat(e.getMessage(), containsString("mapper [shape] has different [precision]"));
        }

        // verify nothing changed
        fieldMapper = docMapper.mappers().getMapper("shape");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy, instanceOf(RecursivePrefixTreeStrategy.class));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getDistErrPct(), equalTo(0.01));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(1d)));
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CCW));

        // correct mapping
        stage2Mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("shape").field("type", "geo_shape")
                .field("tree", "geohash")
                .field("strategy", "recursive")
                .field("precision", "1m")
                .field("tree_levels", 8).field("distance_error_pct", 0.001)
                .field("orientation", "cw").endObject().endObject().endObject().endObject());
        docMapper = mapperService.merge("type", new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE);

        fieldMapper = docMapper.mappers().getMapper("shape");
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

    public void testEmptyName() throws Exception {
        // after 5.x
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("")
            .field("type", "geo_shape")
            .field("tree", "quadtree")
            .endObject().endObject()
            .endObject().endObject());
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type1", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("fieldName is required"));
        assertFieldWarnings("tree");
    }

    public void testSerializeDefaults() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject());
            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            String serialized = toXContentString((LegacyGeoShapeFieldMapper) defaultMapper.mappers().getMapper("location"));
            assertTrue(serialized, serialized.contains("\"precision\":\"50.0m\""));
            assertTrue(serialized, serialized.contains("\"tree_levels\":21"));
        }
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "geohash")
                .endObject().endObject()
                .endObject().endObject());
            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            String serialized = toXContentString((LegacyGeoShapeFieldMapper) defaultMapper.mappers().getMapper("location"));
            assertTrue(serialized, serialized.contains("\"precision\":\"50.0m\""));
            assertTrue(serialized, serialized.contains("\"tree_levels\":9"));
        }
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("tree_levels", "6")
                .endObject().endObject()
                .endObject().endObject());
            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            String serialized = toXContentString((LegacyGeoShapeFieldMapper) defaultMapper.mappers().getMapper("location"));
            assertFalse(serialized, serialized.contains("\"precision\":"));
            assertTrue(serialized, serialized.contains("\"tree_levels\":6"));
        }
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("precision", "6")
                .endObject().endObject()
                .endObject().endObject());
            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            String serialized = toXContentString((LegacyGeoShapeFieldMapper) defaultMapper.mappers().getMapper("location"));
            assertTrue(serialized, serialized.contains("\"precision\":\"6.0m\""));
            assertFalse(serialized, serialized.contains("\"tree_levels\":"));
        }
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("precision", "6m")
                .field("tree_levels", "5")
                .endObject().endObject()
                .endObject().endObject());
            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            String serialized = toXContentString((LegacyGeoShapeFieldMapper) defaultMapper.mappers().getMapper("location"));
            assertTrue(serialized, serialized.contains("\"precision\":\"6.0m\""));
            assertTrue(serialized, serialized.contains("\"tree_levels\":5"));
        }
        assertFieldWarnings("tree", "tree_levels", "precision");
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

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));

        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultPrefixTreeStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.0));
        assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(23));
        assertThat(strategy.isPointsOnly(), equalTo(true));
        // term strategy changes the default for points_only, check that we handle it correctly
        assertThat(toXContentString(geoShapeFieldMapper, false), not(containsString("points_only")));
        assertFieldWarnings("tree", "precision", "strategy");
    }


    public void testPointsOnlyFalseWithTermStrategy() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("tree", "quadtree")
            .field("precision", "10m")
            .field("strategy", "term")
            .field("points_only", false)
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> parser.parse("type1", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("points_only cannot be set to false for term strategy"));
        assertFieldWarnings("tree", "precision", "strategy", "points_only");
    }

    public void testDisallowExpensiveQueries() throws IOException {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.allowExpensiveQueries()).thenReturn(false);
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
                .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(LegacyGeoShapeFieldMapper.class));
        LegacyGeoShapeFieldMapper geoShapeFieldMapper = (LegacyGeoShapeFieldMapper) fieldMapper;


        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> geoShapeFieldMapper.fieldType().geometryQueryBuilder().process(
                        new Point(-10, 10), "location", SpatialStrategy.TERM, ShapeRelation.INTERSECTS, queryShardContext));
        assertEquals("[geo-shape] queries on [PrefixTree geo shapes] cannot be executed when " +
                        "'search.allow_expensive_queries' is set to false.", e.getMessage());
        assertFieldWarnings("tree");
    }

    @Override
    public void testSerialization() throws IOException {
        super.testSerialization();
        assertWarnings("Field parameter [strategy] is deprecated and will be removed in a future version.",
            "Field parameter [tree] is deprecated and will be removed in a future version.",
            "Field parameter [tree_levels] is deprecated and will be removed in a future version.",
            "Field parameter [precision] is deprecated and will be removed in a future version.",
            "Field parameter [distance_error_pct] is deprecated and will be removed in a future version."
            );
    }

    public void testGeoShapeArrayParsing() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_shape")
            .field("tree", "quadtree")
            .endObject()
            .endObject()
            .endObject());

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        BytesReference arrayedDoc = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startArray("shape")
            .startObject()
            .field("type", "Point")
            .startArray("coordinates").value(176.0).value(15.0).endArray()
            .endObject()
            .startObject()
            .field("type", "Point")
            .startArray("coordinates").value(76.0).value(-15.0).endArray()
            .endObject()
            .endArray()
            .endObject()
        );

        SourceToParse sourceToParse = new SourceToParse("test", "1", arrayedDoc, XContentType.JSON);
        ParsedDocument document = mapper.parse(sourceToParse);
        assertThat(document.docs(), hasSize(1));
        IndexableField[] fields = document.docs().get(0).getFields("shape.type");
        assertThat(fields.length, equalTo(2));
        assertFieldWarnings("tree");
    }

    public String toXContentString(LegacyGeoShapeFieldMapper mapper, boolean includeDefaults) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        ToXContent.Params params;
        if (includeDefaults) {
            params = new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"));
        } else {
            params = ToXContent.EMPTY_PARAMS;
        }
        mapper.doXContentBody(builder, includeDefaults, params);
        return Strings.toString(builder.endObject());
    }

    public String toXContentString(LegacyGeoShapeFieldMapper mapper) throws IOException {
        return toXContentString(mapper, true);
    }

    public void testParseSourceValue() {
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
        sourceLookup.setSource(Collections.singletonMap("field", jsonLineString));
        assertEquals(List.of(jsonLineString), mapper.lookupValues(sourceLookup, null));
        assertEquals(List.of(wktLineString), mapper.lookupValues(sourceLookup, "wkt"));

        // Test a list of shapes in geojson format.
        sourceLookup.setSource(Collections.singletonMap("field", List.of(jsonLineString, jsonPoint)));
        assertEquals(List.of(jsonLineString, jsonPoint), mapper.lookupValues(sourceLookup, null));
        assertEquals(List.of(wktLineString, wktPoint), mapper.lookupValues(sourceLookup, "wkt"));

        // Test a single shape in wkt format.
        sourceLookup.setSource(Collections.singletonMap("field", wktLineString));
        assertEquals(List.of(jsonLineString), mapper.lookupValues(sourceLookup, null));
        assertEquals(List.of(wktLineString), mapper.lookupValues(sourceLookup, "wkt"));

        // Test a list of shapes in wkt format.
        sourceLookup.setSource(Collections.singletonMap("field", List.of(wktLineString, wktPoint)));
        assertEquals(List.of(jsonLineString, jsonPoint), mapper.lookupValues(sourceLookup, null));
        assertEquals(List.of(wktLineString, wktPoint), mapper.lookupValues(sourceLookup, "wkt"));
    }
}
