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

import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDefaultConfiguration() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.025d));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoShapeFieldMapper.Defaults.GEOHASH_LEVELS));
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(GeoShapeFieldMapper.Defaults.ORIENTATION));
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("orientation", "left")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        ShapeBuilder.Orientation orientation = ((GeoShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // explicit right orientation test
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("orientation", "right")
                .endObject().endObject()
                .endObject().endObject().string();

        defaultMapper = createIndex("test2").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        orientation = ((GeoShapeFieldMapper)fieldMapper).fieldType().orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
    }

    /**
     * Test that coerce parameter correctly parses
     */
    public void testCoerceParsing() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("coerce", "true")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        boolean coerce = ((GeoShapeFieldMapper)fieldMapper).coerce().value();
        assertThat(coerce, equalTo(true));

        // explicit false coerce test
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("coerce", "false")
                .endObject().endObject()
                .endObject().endObject().string();

        defaultMapper = createIndex("test2").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        coerce = ((GeoShapeFieldMapper)fieldMapper).coerce().value();
        assertThat(coerce, equalTo(false));
    }

    /**
     * Test that ignore_malformed parameter correctly parses
     */
    public void testIgnoreMalformedParsing() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("ignore_malformed", "true")
            .endObject().endObject()
            .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        Explicit<Boolean> ignoreMalformed = ((GeoShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.value(), equalTo(true));

        // explicit false ignore_malformed test
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("ignore_malformed", "false")
            .endObject().endObject()
            .endObject().endObject().string();

        defaultMapper = createIndex("test2").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        ignoreMalformed = ((GeoShapeFieldMapper)fieldMapper).ignoreMalformed();
        assertThat(ignoreMalformed.explicit(), equalTo(true));
        assertThat(ignoreMalformed.value(), equalTo(false));
    }

    public void testGeohashConfiguration() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "geohash")
                    .field("tree_levels", "4")
                    .field("distance_error_pct", "0.1")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.1));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(4));
    }

    public void testQuadtreeConfiguration() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "quadtree")
                    .field("tree_levels", "6")
                    .field("distance_error_pct", "0.5")
                    .field("points_only", true)
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.5));
        assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(6));
        assertThat(strategy.isPointsOnly(), equalTo(true));
    }

    public void testLevelPrecisionConfiguration() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("tree_levels", "6")
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();


            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            // 70m is more precise so it wins
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(70d)));
        }

        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "quadtree")
                    .field("tree_levels", "26")
                    .field("precision", "70m")
                    .endObject().endObject()
                    .endObject().endObject().string();


            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

            // distance_error_pct was not specified so we expect the mapper to take the highest precision between "precision" and
            // "tree_levels" setting distErrPct to 0 to guarantee desired precision
            assertThat(strategy.getDistErrPct(), equalTo(0.0));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            // 70m is less precise so it loses
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(26));
        }

        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("tree_levels", "6")
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            // 70m is more precise so it wins
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(70d)));
        }

        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("tree_levels",  GeoUtils.geoHashLevelsForPrecision(70d)+1)
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            assertThat(strategy.getGrid().getMaxLevels(),  equalTo(GeoUtils.geoHashLevelsForPrecision(70d)+1));
        }

        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("tree_levels", GeoUtils.quadTreeLevelsForPrecision(70d)+1)
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(70d)+1));
        }
    }

    public void testPointsOnlyOption() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "geohash")
                .field("points_only", true)
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.isPointsOnly(), equalTo(true));
    }

    public void testLevelDefaults() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();


            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            /* 50m is default */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(50d)));
        }

        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = parser.parse("type1", new CompressedXContent(mapping));
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            /* 50m is default */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(50d)));
        }
    }

    public void testGeoShapeMapperMerge() throws Exception {
        String stage1Mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("shape").field("type", "geo_shape").field("tree", "geohash").field("strategy", "recursive")
                .field("precision", "1m").field("tree_levels", 8).field("distance_error_pct", 0.01).field("orientation", "ccw")
                .endObject().endObject().endObject().endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper docMapper = mapperService.merge("type", new CompressedXContent(stage1Mapping), MapperService.MergeReason.MAPPING_UPDATE, false);
        String stage2Mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("shape").field("type", "geo_shape").field("tree", "quadtree")
                .field("strategy", "term").field("precision", "1km").field("tree_levels", 26).field("distance_error_pct", 26)
                .field("orientation", "cw").endObject().endObject().endObject().endObject().string();
        try {
            mapperService.merge("type", new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [shape] has different [strategy]"));
            assertThat(e.getMessage(), containsString("mapper [shape] has different [tree]"));
            assertThat(e.getMessage(), containsString("mapper [shape] has different [tree_levels]"));
            assertThat(e.getMessage(), containsString("mapper [shape] has different [precision]"));
        }

        // verify nothing changed
        FieldMapper fieldMapper = docMapper.mappers().getMapper("shape");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

        assertThat(strategy, instanceOf(RecursivePrefixTreeStrategy.class));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getDistErrPct(), equalTo(0.01));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(1d)));
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CCW));

        // correct mapping
        stage2Mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("shape").field("type", "geo_shape").field("precision", "1m")
                .field("tree_levels", 8).field("distance_error_pct", 0.001).field("orientation", "cw").endObject().endObject().endObject().endObject().string();
        docMapper = mapperService.merge("type", new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        fieldMapper = docMapper.mappers().getMapper("shape");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        strategy = geoShapeFieldMapper.fieldType().defaultStrategy();

        assertThat(strategy, instanceOf(RecursivePrefixTreeStrategy.class));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getDistErrPct(), equalTo(0.001));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(1d)));
        assertThat(geoShapeFieldMapper.fieldType().orientation(), equalTo(ShapeBuilder.Orientation.CW));
    }

    public void testEmptyName() throws Exception {
        // after 5.x
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("")
            .field("type", "geo_shape")
            .endObject().endObject()
            .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type1", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

}
