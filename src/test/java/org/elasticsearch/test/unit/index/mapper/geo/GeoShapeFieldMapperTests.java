package org.elasticsearch.test.unit.index.mapper.geo;

import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.io.WKBReader;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.geo.GeoJSONShapeSerializer;
import org.elasticsearch.common.lucene.spatial.SpatialStrategy;
import org.elasticsearch.common.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.elasticsearch.common.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.elasticsearch.common.geo.ShapeBuilder.newRectangle;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

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
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.GeoShapeIndexFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = ((GeoShapeFieldMapper.GeoShapeIndexFieldMapper) fieldMapper).geoMapper();
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
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.GeoShapeIndexFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = ((GeoShapeFieldMapper.GeoShapeIndexFieldMapper) fieldMapper).geoMapper();
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
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.GeoShapeIndexFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = ((GeoShapeFieldMapper.GeoShapeIndexFieldMapper) fieldMapper).geoMapper();
        SpatialStrategy strategy = geoShapeFieldMapper.spatialStrategy();

        assertThat(strategy.getDistanceErrorPct(), equalTo(0.5));
        assertThat(strategy.getPrefixTree(), instanceOf(QuadPrefixTree.class));
        assertThat(strategy.getPrefixTree().getMaxLevels(), equalTo(6));
    }

    /**
     * Test that setting the "wkb" attribute in the mapping is correctly serialized
     * in the index.
     */
    @Test
    public void testWkbStorage() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("wkb", "true")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        Shape shape = newRectangle().topLeft(-45, 45).bottomRight(45, -45).build();

        XContentBuilder docBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("location");
        GeoJSONShapeSerializer.serialize(shape, docBuilder);
        docBuilder.endObject().endObject();

        ParsedDocument doc = defaultMapper.parse("type1", "1", docBuilder.bytes());
        String base64 = doc.rootDoc().get("location.wkb");
        Shape testingGeom = new JtsGeometry(new WKBReader().read(Base64.decode(base64)), JtsSpatialContext.GEO, false);
        // There's not a great way to test for shape equality, especially between
        // JTS Geometrys and spatial4j Shapes.
        assertThat(testingGeom.relate(shape), not(SpatialRelation.DISJOINT));
    }
}
