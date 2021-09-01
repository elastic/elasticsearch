/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.geo;

import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class GeoShapeIntegTestCase extends ESIntegTestCase {

    protected abstract void getGeoShapeMapping(XContentBuilder b) throws IOException;

    protected abstract Version getVersion();

    protected abstract boolean allowExpensiveQueries();

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    /**
     * Test that orientation parameter correctly persists across cluster restart
     */
    public void testOrientationPersistence() throws Exception {
        String idxName = "orientation";
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("location");
        getGeoShapeMapping(mapping);
        mapping.field("orientation", "left").endObject().endObject().endObject();

        // create index
        assertAcked(prepareCreate(idxName).setMapping(mapping).setSettings(settings(getVersion()).build()));

        mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("location");
        getGeoShapeMapping(mapping);
        mapping.field("orientation", "right").endObject().endObject().endObject();

        assertAcked(prepareCreate(idxName+"2").setMapping(mapping).setSettings(settings(getVersion()).build()));
        ensureGreen(idxName, idxName+"2");

        internalCluster().fullRestart();
        ensureGreen(idxName, idxName+"2");

        // left orientation test
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName));
        IndexService indexService = indicesService.indexService(resolveIndex(idxName));
        MappedFieldType fieldType = indexService.mapperService().fieldType("location");
        assertThat(fieldType, instanceOf(AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType.class));

        AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType<?> gsfm =
            (AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType<?>)fieldType;
        Orientation orientation = gsfm.orientation();
        assertThat(orientation, equalTo(Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.LEFT));
        assertThat(orientation, equalTo(Orientation.CW));

        // right orientation test
        indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName+"2"));
        indexService = indicesService.indexService(resolveIndex((idxName+"2")));
        fieldType = indexService.mapperService().fieldType("location");
        assertThat(fieldType, instanceOf(AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType.class));

        gsfm = (AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType<?>)fieldType;
        orientation = gsfm.orientation();
        assertThat(orientation, equalTo(Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.RIGHT));
        assertThat(orientation, equalTo(Orientation.CCW));
    }

    /**
     * Test that ignore_malformed on GeoShapeFieldMapper does not fail the entire document
     */
    public void testIgnoreMalformed() throws Exception {
        // create index
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("shape");
        getGeoShapeMapping(mapping);
        mapping.field("ignore_malformed", true).endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setMapping(mapping).setSettings(settings(getVersion()).build()));
        ensureGreen();

        // test self crossing ccw poly not crossing dateline
        String polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
            .startArray("coordinates")
            .startArray()
            .startArray().value(176.0).value(15.0).endArray()
            .startArray().value(-177.0).value(10.0).endArray()
            .startArray().value(-177.0).value(-10.0).endArray()
            .startArray().value(176.0).value(-15.0).endArray()
            .startArray().value(-177.0).value(15.0).endArray()
            .startArray().value(172.0).value(0.0).endArray()
            .startArray().value(176.0).value(15.0).endArray()
            .endArray()
            .endArray()
            .endObject());

        indexRandom(true, client().prepareIndex("test").setId("0").setSource("shape",
            polygonGeoJson));
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    /**
     * Test that the indexed shape routing can be provided if it is required
     */
    public void testIndexShapeRouting() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_doc").startObject("_routing").field("required", true).endObject()
            .startObject("properties").startObject("shape");
        getGeoShapeMapping(mapping);
        mapping.endObject().endObject().endObject().endObject();

        // create index
        assertAcked(prepareCreate("test").setMapping(mapping).setSettings(settings(getVersion()).build()));
        ensureGreen();

        String source = "{\n" +
            "    \"shape\" : {\n" +
            "        \"type\" : \"bbox\",\n" +
            "        \"coordinates\" : [[-45.0, 45.0], [45.0, -45.0]]\n" +
            "    }\n" +
            "}";

        indexRandom(true, client().prepareIndex("test").setId("0").setSource(source, XContentType.JSON).setRouting("ABC"));

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(
            geoShapeQuery("shape", "0").indexedShapeIndex("test").indexedShapeRouting("ABC")
        ).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testIndexPolygonDateLine() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("shape");
        getGeoShapeMapping(mapping);
        mapping.endObject().endObject().endObject();

        // create index
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(settings(getVersion()).build()).setMapping(mapping).get());
        ensureGreen();

        String source = "{\n" +
            "    \"shape\" : \"POLYGON((179 0, -179 0, -179 2, 179 2, 179 0))\"" +
            "}";

        indexRandom(true, client().prepareIndex("test").setId("0").setSource(source, XContentType.JSON));

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape", new Point(-179.75, 1))).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape", new Point(90, 1))).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape", new Point(-180, 1))).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape", new Point(180, 1))).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testDisallowExpensiveQueries() throws InterruptedException, IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("shape");
        getGeoShapeMapping(mapping);
        mapping.endObject().endObject().endObject();

        // create index
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(settings(getVersion()).build()).setMapping(mapping).get());
        ensureGreen();

        String source = "{\n" +
            "    \"shape\" : {\n" +
            "        \"type\" : \"bbox\",\n" +
            "        \"coordinates\" : [[-45.0, 45.0], [45.0, -45.0]]\n" +
            "    }\n" +
            "}";

        indexRandom(true, client().prepareIndex("test").setId("0").setSource(source, XContentType.JSON));
        refresh();

        try {
            // Execute with search.allow_expensive_queries to false
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", false));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            SearchRequestBuilder builder = client().prepareSearch("test").setQuery(geoShapeQuery("shape",
                new Circle(0, 0, 77000)));
            if (allowExpensiveQueries()) {
                assertThat(builder.get().getHits().getTotalHits().value, equalTo(1L));
            } else {
                ElasticsearchException e = expectThrows(ElasticsearchException.class, builder::get);
                assertEquals("[geo-shape] queries on [PrefixTree geo shapes] cannot be executed when " +
                    "'search.allow_expensive_queries' is set to false.", e.getCause().getMessage());
            }

            // Set search.allow_expensive_queries to "null"
            updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
            assertThat(builder.get().getHits().getTotalHits().value, equalTo(1L));

            // Set search.allow_expensive_queries to "true"
            updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
            assertThat(builder.get().getHits().getTotalHits().value, equalTo(1L));
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }
    }

    public void testShapeRelations() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("area");
        getGeoShapeMapping(mapping);
        mapping.endObject().endObject().endObject();

        final Version version = getVersion();
        CreateIndexRequestBuilder mappingRequest = client().admin().indices().prepareCreate("shapes")
            .setMapping(mapping).setSettings(settings(version).build());
        mappingRequest.get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        // Create a multipolygon with two polygons. The first is an rectangle of size 10x10
        // with a hole of size 5x5 equidistant from all sides. This hole in turn contains
        // the second polygon of size 4x4 equidistant from all sites
        List<Polygon> polygons = List.of(
            new Polygon(
                new LinearRing(
                    new double[] {-10, -10, 10, 10, -10},
                    new double[] {-10, 10, 10, -10, -10}
                ),
                List.of(
                    new LinearRing(
                        new double[] {-5, -5, 5, 5, -5},
                        new double[] {-5, 5, 5, -5, -5}
                    )
                )
            ),
            new Polygon(
                new LinearRing(
                    new double[] {-4, -4, 4, 4, -4},
                    new double[] {-4, 4, 4, -4, -4}
                )
            )
        );

        BytesReference data = BytesReference.bytes(
            jsonBuilder().startObject().field("area", WellKnownText.toWKT(new MultiPolygon(polygons))).endObject()
        );

        client().prepareIndex("shapes").setId("1").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        // Point in polygon
        SearchResponse result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new Point(3, 3)))
            .get();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        // Point in polygon hole
        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new Point(4.5, 4.5)))
            .get();
        assertHitCount(result, 0);

        // by definition the border of a polygon belongs to the inner
        // so the border of a polygons hole also belongs to the inner
        // of the polygon NOT the hole

        // Point on polygon border
        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new Point(10.0, 5.0)))
            .get();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        // Point on hole border
        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new Point(5.0, 2.0)))
            .get();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));


        // Point not in polygon
        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoDisjointQuery("area", new Point(3, 3)))
            .get();
        assertHitCount(result, 0);

        // Point in polygon hole
        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoDisjointQuery("area", new Point(4.5, 4.5)))
            .get();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        // Create a polygon that fills the empty area of the polygon defined above
        Polygon inverse = new Polygon(
            new LinearRing(
                new double[] {-5, -5, 5, 5, -5},
                new double[] {-5, 5, 5, -5, -5}
            ),
            List.of(
                new LinearRing(
                    new double[] {-4, -4, 4, 4, -4},
                    new double[] {-4, 4, 4, -4, -4}
                )
            )
        );

        data = BytesReference.bytes(jsonBuilder().startObject().field("area", WellKnownText.toWKT(inverse)).endObject());
        client().prepareIndex("shapes").setId("2").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        // re-check point on polygon hole
        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new Point(4.5, 4.5)))
            .get();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("2"));


        // Polygon WithIn Polygon
        Polygon WithIn = new Polygon(
            new LinearRing(
                new double[] {-30, -30, 30, 30, -30},
                new double[] {-30, 30, 30, -30, -30}
            )
        );

        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoWithinQuery("area", WithIn))
            .get();
        assertHitCount(result, 2);

        // Create a polygon crossing longitude 180.
        Polygon crossing = new Polygon(
            new LinearRing(
                new double[] {170, 190, 190, 170, 170},
                new double[] {-10, -10, 10, 10, -10}
            )
        );

        data = BytesReference.bytes(jsonBuilder().startObject().field("area", WellKnownText.toWKT(crossing)).endObject());
        client().prepareIndex("shapes").setId("1").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        // Create a polygon crossing longitude 180 with hole.
        crossing = new Polygon(
            new LinearRing(
                new double[] {170, 190, 190, 170, 170},
                new double[] {-10, -10, 10, 10, -10}
            ),
            List.of(
                new LinearRing(
                    new double[] {175, 185, 185, 175, 175},
                    new double[] {-5, -5, 5, 5, -5}
                )
            )
        );

        data = BytesReference.bytes(jsonBuilder().startObject().field("area", WellKnownText.toWKT(crossing)).endObject());
        client().prepareIndex("shapes").setId("1").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new Point(174, -4)))
            .get();
        assertHitCount(result, 1);

        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new Point(-174, -4)))
            .get();
        assertHitCount(result, 1);

        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new Point(180, -4)))
            .get();
        assertHitCount(result, 0);

        result = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new Point(180, -6)))
            .get();
        assertHitCount(result, 1);
    }

    public void testBulk() throws Exception {
        byte[] bulkAction = unZipData("/org/elasticsearch/search/geo/gzippedmap.gz");
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("pin")
            .field("type", "geo_point");
        xContentBuilder.field("store", true)
            .endObject()
            .startObject("location");
        getGeoShapeMapping(xContentBuilder);
        xContentBuilder.field("ignore_malformed", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate("countries").setSettings(settings)
            .setMapping(xContentBuilder).get();
        BulkResponse bulk = client().prepareBulk().add(bulkAction, 0, bulkAction.length, null, xContentBuilder.contentType()).get();

        for (BulkItemResponse item : bulk.getItems()) {
            assertFalse("unable to index data", item.isFailed());
        }

        client().admin().indices().prepareRefresh().get();
        String key = "DE";

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchQuery("_id", key))
            .get();

        assertHitCount(searchResponse, 1);

        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), equalTo(key));
        }

        SearchResponse world = client().prepareSearch().addStoredField("pin").setQuery(
            geoBoundingBoxQuery("pin").setCorners(90, -179.99999, -90, 179.99999)
        ).get();

        assertHitCount(world, 53);

        SearchResponse distance = client().prepareSearch().addStoredField("pin").setQuery(
            geoDistanceQuery("pin").distance("425km").point(51.11, 9.851)
        ).get();

        assertHitCount(distance, 5);
        GeoPoint point = new GeoPoint();
        for (SearchHit hit : distance.getHits()) {
            String name = hit.getId();
            point.resetFromString(hit.getFields().get("pin").getValue());
            double dist = distance(point.getLat(), point.getLon(), 51.11, 9.851);

            assertThat("distance to '" + name + "'", dist, lessThanOrEqualTo(425000d));
            assertThat(name, anyOf(equalTo("CZ"), equalTo("DE"), equalTo("BE"), equalTo("NL"), equalTo("LU")));
            if (key.equals(name)) {
                assertThat(dist, closeTo(0d, 0.1d));
            }
        }
    }

    private String findNodeName(String index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable shard = state.getRoutingTable().index(index).shard(0);
        String nodeId = shard.assignedShards().get(0).currentNodeId();
        return state.getNodes().get(nodeId).getName();
    }

    private byte[] unZipData(String path) throws IOException {
        InputStream is = Streams.class.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPInputStream in = new GZIPInputStream(is);
        Streams.copy(in, out);

        is.close();
        out.close();

        return out.toByteArray();
    }

    private double distance(double lat1, double lon1, double lat2, double lon2) {
        return SloppyMath.haversinMeters(lat1, lon1, lat2, lon2);
    }
}
