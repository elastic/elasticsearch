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

import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isIn;

public class GeoShapeFieldMapperTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testDefaultConfiguration() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(GeoShapeFieldMapper.Defaults.DISTANCE_ERROR_PCT));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoShapeFieldMapper.Defaults.GEOHASH_LEVELS));
        assertThat(geoShapeFieldMapper.orientation(), equalTo(GeoShapeFieldMapper.Defaults.ORIENTATION));
    }

    /**
     * Test that orientation parameter correctly parses
     * @throws IOException
     */
    public void testOrientationParsing() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("orientation", "left")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        ShapeBuilder.Orientation orientation = ((GeoShapeFieldMapper)fieldMapper).orientation();
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

        defaultMapper = createIndex("test2").mapperService().documentMapperParser().parse(mapping);
        fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        orientation = ((GeoShapeFieldMapper)fieldMapper).orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
    }

    @Test
    public void testGeohashConfiguration() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "geohash")
                    .field("tree_levels", "4")
                    .field("distance_error_pct", "0.1")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.1));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(4));
    }

    @Test
    public void testQuadtreeConfiguration() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "quadtree")
                    .field("tree_levels", "6")
                    .field("distance_error_pct", "0.5")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.5));
        assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(6));
    }
    
    @Test
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

            
            DocumentMapper defaultMapper = parser.parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

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


            DocumentMapper defaultMapper = parser.parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

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

            DocumentMapper defaultMapper = parser.parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

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

            DocumentMapper defaultMapper = parser.parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

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

            DocumentMapper defaultMapper = parser.parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(70d)+1)); 
        }
    }
    
    @Test
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

            
            DocumentMapper defaultMapper = parser.parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

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

            DocumentMapper defaultMapper = parser.parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("location");
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            /* 50m is default */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(50d))); 
        }
    }

    @Test
    public void testGeoShapeMapperMerge() throws Exception {
        String stage1Mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("shape").field("type", "geo_shape").field("tree", "geohash").field("strategy", "recursive")
                .field("precision", "1m").field("tree_levels", 8).field("distance_error_pct", 0.01).field("orientation", "ccw")
                .endObject().endObject().endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper stage1 = parser.parse(stage1Mapping);
        String stage2Mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("shape").field("type", "geo_shape").field("tree", "quadtree")
                .field("strategy", "term").field("precision", "1km").field("tree_levels", 26).field("distance_error_pct", 26)
                .field("orientation", "cw").endObject().endObject().endObject().endObject().string();
        DocumentMapper stage2 = parser.parse(stage2Mapping);

        MergeResult mergeResult = stage1.merge(stage2.mapping(), false);
        // check correct conflicts
        assertThat(mergeResult.hasConflicts(), equalTo(true));
        assertThat(mergeResult.buildConflicts().length, equalTo(3));
        ArrayList conflicts = new ArrayList<>(Arrays.asList(mergeResult.buildConflicts()));
        assertThat("mapper [shape] has different strategy", isIn(conflicts));
        assertThat("mapper [shape] has different tree", isIn(conflicts));
        assertThat("mapper [shape] has different tree_levels or precision", isIn(conflicts));

        // verify nothing changed
        FieldMapper fieldMapper = stage1.mappers().getMapper("shape");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

        assertThat(strategy, instanceOf(RecursivePrefixTreeStrategy.class));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getDistErrPct(), equalTo(0.01));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(1d)));
        assertThat(geoShapeFieldMapper.orientation(), equalTo(ShapeBuilder.Orientation.CCW));

        // correct mapping
        stage2Mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("shape").field("type", "geo_shape").field("precision", "1m")
                .field("distance_error_pct", 0.001).field("orientation", "cw").endObject().endObject().endObject().endObject().string();
        stage2 = parser.parse(stage2Mapping);
        mergeResult = stage1.merge(stage2.mapping(), false);

        // verify mapping changes, and ensure no failures
        assertThat(mergeResult.hasConflicts(), equalTo(false));

        fieldMapper = stage1.mappers().getMapper("shape");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        strategy = geoShapeFieldMapper.defaultStrategy();

        assertThat(strategy, instanceOf(RecursivePrefixTreeStrategy.class));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getDistErrPct(), equalTo(0.001));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(1d)));
        assertThat(geoShapeFieldMapper.orientation(), equalTo(ShapeBuilder.Orientation.CW));
    }
}
