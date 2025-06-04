/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.geo;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Streams;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractGeometryQueryBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public abstract class BaseShapeIntegTestCase<T extends AbstractGeometryQueryBuilder<T>> extends ESIntegTestCase {

    protected abstract SpatialQueryBuilders<T> queryBuilder();

    protected abstract String getFieldTypeName();

    /**
     * Provides the content of the mapping. Typically, it adds the type and any other attribute
     * if necessary.
     */
    protected abstract void getGeoShapeMapping(XContentBuilder b) throws IOException;

    /**
     * Provides a supported version when the mapping was created.
     */
    protected abstract IndexVersion randomSupportedVersion();

    /**
     * If this field is allowed to be executed when setting allow_expensive_queries us set to false.
     */
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
        assertAcked(prepareCreate(idxName).setMapping(mapping).setSettings(settings(randomSupportedVersion()).build()));

        mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("location");
        getGeoShapeMapping(mapping);
        mapping.field("orientation", "right").endObject().endObject().endObject();

        assertAcked(prepareCreate(idxName + "2").setMapping(mapping).setSettings(settings(randomSupportedVersion()).build()));
        ensureGreen(idxName, idxName + "2");

        internalCluster().fullRestart();
        ensureGreen(idxName, idxName + "2");

        // left orientation test
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName));
        IndexService indexService = indicesService.indexService(resolveIndex(idxName));
        MappedFieldType fieldType = indexService.mapperService().fieldType("location");
        assertThat(fieldType, instanceOf(AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType.class));

        AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType<?> gsfm =
            (AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType<?>) fieldType;
        Orientation orientation = gsfm.orientation();
        assertThat(orientation, equalTo(Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.LEFT));
        assertThat(orientation, equalTo(Orientation.CW));

        // right orientation test
        indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName + "2"));
        indexService = indicesService.indexService(resolveIndex((idxName + "2")));
        fieldType = indexService.mapperService().fieldType("location");
        assertThat(fieldType, instanceOf(AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType.class));

        gsfm = (AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType<?>) fieldType;
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
        assertAcked(prepareCreate("test").setMapping(mapping).setSettings(settings(randomSupportedVersion()).build()));
        ensureGreen();

        // test self crossing ccw poly not crossing dateline
        String polygonGeoJson = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray()
                .value(176.0)
                .value(15.0)
                .endArray()
                .startArray()
                .value(-177.0)
                .value(10.0)
                .endArray()
                .startArray()
                .value(-177.0)
                .value(-10.0)
                .endArray()
                .startArray()
                .value(176.0)
                .value(-15.0)
                .endArray()
                .startArray()
                .value(-177.0)
                .value(15.0)
                .endArray()
                .startArray()
                .value(172.0)
                .value(0.0)
                .endArray()
                .startArray()
                .value(176.0)
                .value(15.0)
                .endArray()
                .endArray()
                .endArray()
                .endObject()
        );

        indexRandom(true, client().prepareIndex("test").setId("0").setSource("shape", polygonGeoJson));
        assertHitCount(client().prepareSearch("test").setQuery(matchAllQuery()), 1L);
    }

    /**
     * Test that the indexed shape routing can be provided if it is required
     */
    public void testIndexShapeRouting() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_routing")
            .field("required", true)
            .endObject()
            .startObject("properties")
            .startObject("shape");
        getGeoShapeMapping(mapping);
        mapping.endObject().endObject().endObject().endObject();

        // create index
        assertAcked(prepareCreate("test").setMapping(mapping).setSettings(settings(randomSupportedVersion()).build()));
        ensureGreen();

        String source = """
            {
                "shape" : {
                    "type" : "bbox",
                    "coordinates" : [[-45.0, 45.0], [45.0, -45.0]]
                }
            }""";

        indexRandom(true, client().prepareIndex("test").setId("0").setSource(source, XContentType.JSON).setRouting("ABC"));
        assertHitCount(
            client().prepareSearch("test")
                .setQuery(queryBuilder().shapeQuery("shape", "0").indexedShapeIndex("test").indexedShapeRouting("ABC")),
            1L
        );
    }

    public void testDisallowExpensiveQueries() throws InterruptedException, IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("shape");
        getGeoShapeMapping(mapping);
        mapping.endObject().endObject().endObject();

        // create index
        assertAcked(indicesAdmin().prepareCreate("test").setSettings(settings(randomSupportedVersion()).build()).setMapping(mapping).get());
        ensureGreen();

        String source = """
            {
                "shape" : {
                    "type" : "bbox",
                    "coordinates" : [[-45.0, 45.0], [45.0, -45.0]]
                }
            }""";

        indexRandom(true, prepareIndex("test").setId("0").setSource(source, XContentType.JSON));
        refresh();

        try {
            // Execute with search.allow_expensive_queries to false
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", false));

            SearchRequestBuilder builder = client().prepareSearch("test")
                .setQuery(queryBuilder().shapeQuery("shape", new Circle(0, 0, 77000)));
            if (allowExpensiveQueries()) {
                assertHitCount(builder, 1L);
            } else {
                ElasticsearchException e = expectThrows(ElasticsearchException.class, builder::get);
                assertEquals(
                    "[geo-shape] queries on [PrefixTree geo shapes] cannot be executed when "
                        + "'search.allow_expensive_queries' is set to false.",
                    e.getCause().getMessage()
                );
            }

            // Set search.allow_expensive_queries to "null"
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertHitCount(builder, 1);

            // Set search.allow_expensive_queries to "true"
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertHitCount(builder, 1L);
        } finally {
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
        }
    }

    public void testShapeRelations() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("area");
        getGeoShapeMapping(mapping);
        mapping.endObject().endObject().endObject();

        final IndexVersion version = randomSupportedVersion();
        CreateIndexRequestBuilder mappingRequest = indicesAdmin().prepareCreate("shapes")
            .setMapping(mapping)
            .setSettings(settings(version).build());
        mappingRequest.get();
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        // Create a multipolygon with two polygons. The first is an rectangle of size 10x10
        // with a hole of size 5x5 equidistant from all sides. This hole in turn contains
        // the second polygon of size 4x4 equidistant from all sites
        List<Polygon> polygons = List.of(
            new Polygon(
                new LinearRing(new double[] { -10, -10, 10, 10, -10 }, new double[] { -10, 10, 10, -10, -10 }),
                List.of(new LinearRing(new double[] { -5, -5, 5, 5, -5 }, new double[] { -5, 5, 5, -5, -5 }))
            ),
            new Polygon(new LinearRing(new double[] { -4, -4, 4, 4, -4 }, new double[] { -4, 4, 4, -4, -4 }))
        );

        BytesReference data = BytesReference.bytes(
            jsonBuilder().startObject().field("area", WellKnownText.toWKT(new MultiPolygon(polygons))).endObject()
        );

        prepareIndex("shapes").setId("1").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        // Point in polygon
        assertResponse(
            client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryBuilder().intersectionQuery("area", new Point(3, 3))),
            response -> {
                assertHitCount(response, 1L);
                assertFirstHit(response, hasId("1"));
            }
        );

        // Point in polygon hole
        assertHitCount(
            client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryBuilder().intersectionQuery("area", new Point(4.5, 4.5))),
            0
        );

        // by definition the border of a polygon belongs to the inner
        // so the border of a polygons hole also belongs to the inner
        // of the polygon NOT the hole

        // Point on polygon border
        assertResponse(
            client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(queryBuilder().intersectionQuery("area", new Point(10.0, 5.0))),
            response -> {
                assertHitCount(response, 1L);
                assertFirstHit(response, hasId("1"));
            }
        );

        // Point on hole border
        assertResponse(
            client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryBuilder().intersectionQuery("area", new Point(5.0, 2.0))),
            response -> {
                assertHitCount(response, 1L);
                assertFirstHit(response, hasId("1"));
            }
        );

        // Point not in polygon
        assertHitCount(
            client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryBuilder().disjointQuery("area", new Point(3, 3))),
            0
        );

        // Point in polygon hole
        assertResponse(
            client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryBuilder().disjointQuery("area", new Point(4.5, 4.5))),
            response -> {
                assertHitCount(response, 1L);
                assertFirstHit(response, hasId("1"));
            }
        );

        // Create a polygon that fills the empty area of the polygon defined above
        Polygon inverse = new Polygon(
            new LinearRing(new double[] { -5, -5, 5, 5, -5 }, new double[] { -5, 5, 5, -5, -5 }),
            List.of(new LinearRing(new double[] { -4, -4, 4, 4, -4 }, new double[] { -4, 4, 4, -4, -4 }))
        );

        data = BytesReference.bytes(jsonBuilder().startObject().field("area", WellKnownText.toWKT(inverse)).endObject());
        prepareIndex("shapes").setId("2").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        // re-check point on polygon hole
        assertResponse(
            client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryBuilder().intersectionQuery("area", new Point(4.5, 4.5))),
            response -> {
                assertHitCount(response, 1L);
                assertFirstHit(response, hasId("2"));
            }
        );

        // Polygon WithIn Polygon
        Polygon WithIn = new Polygon(new LinearRing(new double[] { -30, -30, 30, 30, -30 }, new double[] { -30, 30, 30, -30, -30 }));

        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryBuilder().withinQuery("area", WithIn)), 2);

        // Create a polygon crossing longitude 180.
        Polygon crossing = new Polygon(new LinearRing(new double[] { 170, 190, 190, 170, 170 }, new double[] { -10, -10, 10, 10, -10 }));

        data = BytesReference.bytes(jsonBuilder().startObject().field("area", WellKnownText.toWKT(crossing)).endObject());
        prepareIndex("shapes").setId("1").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        // Create a polygon crossing longitude 180 with hole.
        crossing = new Polygon(
            new LinearRing(new double[] { 170, 190, 190, 170, 170 }, new double[] { -10, -10, 10, 10, -10 }),
            List.of(new LinearRing(new double[] { 175, 185, 185, 175, 175 }, new double[] { -5, -5, 5, 5, -5 }))
        );

        data = BytesReference.bytes(jsonBuilder().startObject().field("area", WellKnownText.toWKT(crossing)).endObject());
        prepareIndex("shapes").setId("1").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        assertHitCount(
            client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryBuilder().intersectionQuery("area", new Point(174, -4))),
            1L
        );

        // In geo coordinates the polygon wraps the dateline, so we need to search within valid longitude ranges
        double xWrapped = getFieldTypeName().contains("geo") ? -174 : 186;
        assertHitCount(
            client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(queryBuilder().intersectionQuery("area", new Point(xWrapped, -4))),
            1L
        );
        assertHitCount(
            client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryBuilder().intersectionQuery("area", new Point(180, -4))),
            0L
        );
        assertHitCount(
            client().prepareSearch().setQuery(matchAllQuery()).setPostFilter(queryBuilder().intersectionQuery("area", new Point(180, -6))),
            1L
        );
    }

    public void testBulk() throws Exception {
        byte[] bulkAction = unZipData("/org/elasticsearch/search/geo/gzippedmap.gz");
        IndexVersion version = randomSupportedVersion();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("pin")
            .field("type", getFieldTypeName());
        xContentBuilder.field("store", true).endObject().startObject("location");
        getGeoShapeMapping(xContentBuilder);
        xContentBuilder.field("ignore_malformed", true).endObject().endObject().endObject().endObject();

        client().admin().indices().prepareCreate("countries").setSettings(settings).setMapping(xContentBuilder).get();
        BulkResponse bulk = client().prepareBulk().add(bulkAction, 0, bulkAction.length, null, xContentBuilder.contentType()).get();

        for (BulkItemResponse item : bulk.getItems()) {
            assertFalse("unable to index data: " + item.getFailureMessage(), item.isFailed());
        }

        assertNoFailures(client().admin().indices().prepareRefresh().get());
        String key = "DE";

        assertResponse(client().prepareSearch().setQuery(matchQuery("_id", key)), response -> {
            assertHitCount(response, 1);
            for (SearchHit hit : response.getHits()) {
                assertThat(hit.getId(), equalTo(key));
            }
        });

        // We extract this to another method to allow some tests to ignore this part
        doDistanceAndBoundingBoxTest(key);
    }

    protected abstract void doDistanceAndBoundingBoxTest(String key);

    private static String findNodeName(String index) {
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
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

        return convertTestData(out);
    }

    /** Override this method if there is need to modify the test data for specific tests */
    protected byte[] convertTestData(ByteArrayOutputStream out) {
        return out.toByteArray();
    }
}
