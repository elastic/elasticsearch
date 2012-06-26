package org.elasticsearch.test.unit.index.mapper.geo;

import org.elasticsearch.common.lucene.spatial.SpatialStrategy;
import org.elasticsearch.common.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.elasticsearch.common.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeFieldMapperTests {

    @Test
    public void testDefaultConfiguration() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        SpatialStrategy strategy = geoShapeFieldMapper.spatialStrategy();

        assertThat(strategy.getDistanceErrorPct(), equalTo(GeoShapeFieldMapper.Defaults.DISTANCE_ERROR_PCT));
        assertThat(strategy.getPrefixTree(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getPrefixTree().getMaxLevels(), equalTo(GeoShapeFieldMapper.Defaults.GEOHASH_LEVELS));
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

        DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        SpatialStrategy strategy = geoShapeFieldMapper.spatialStrategy();

        assertThat(strategy.getDistanceErrorPct(), equalTo(0.1));
        assertThat(strategy.getPrefixTree(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getPrefixTree().getMaxLevels(), equalTo(4));
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

        DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        SpatialStrategy strategy = geoShapeFieldMapper.spatialStrategy();

        assertThat(strategy.getDistanceErrorPct(), equalTo(0.5));
        assertThat(strategy.getPrefixTree(), instanceOf(QuadPrefixTree.class));
        assertThat(strategy.getPrefixTree().getMaxLevels(), equalTo(6));
    }
}
