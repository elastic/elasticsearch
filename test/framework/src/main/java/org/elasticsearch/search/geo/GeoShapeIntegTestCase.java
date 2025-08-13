/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.geo;

import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class GeoShapeIntegTestCase extends BaseShapeIntegTestCase<GeoShapeQueryBuilder> {

    private final SpatialQueryBuilders<GeoShapeQueryBuilder> geoShapeQueryBuilder = SpatialQueryBuilders.GEO;

    @Override
    protected SpatialQueryBuilders<GeoShapeQueryBuilder> queryBuilder() {
        return geoShapeQueryBuilder;
    }

    @Override
    protected String getFieldTypeName() {
        return "geo_point";
    }

    public void testIndexPolygonDateLine() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("shape");
        getGeoShapeMapping(mapping);
        mapping.endObject().endObject().endObject();

        // create index
        assertAcked(indicesAdmin().prepareCreate("test").setSettings(settings(randomSupportedVersion()).build()).setMapping(mapping).get());
        ensureGreen();

        String source = """
            {
              "shape": "POLYGON((179 0, -179 0, -179 2, 179 2, 179 0))"
            }""";

        indexRandom(true, prepareIndex("test").setId("0").setSource(source, XContentType.JSON));

        assertHitCount(prepareSearch("test").setQuery(geoShapeQuery("shape", new Point(-179.75, 1))), 1L);
        assertHitCount(prepareSearch("test").setQuery(geoShapeQuery("shape", new Point(90, 1))), 0L);
        assertHitCount(prepareSearch("test").setQuery(geoShapeQuery("shape", new Point(-180, 1))), 1L);
        assertHitCount(prepareSearch("test").setQuery(geoShapeQuery("shape", new Point(180, 1))), 1L);

    }

    /** The testBulk method uses this only for Geo-specific tests */
    protected void doDistanceAndBoundingBoxTest(String key) {
        assertHitCount(
            prepareSearch().addStoredField("pin").setQuery(geoBoundingBoxQuery("pin").setCorners(90, -179.99999, -90, 179.99999)),
            53
        );

        assertResponse(
            prepareSearch().addStoredField("pin").setQuery(geoDistanceQuery("pin").distance("425km").point(51.11, 9.851)),
            response -> {
                assertHitCount(response, 5L);
                GeoPoint point = new GeoPoint();
                for (SearchHit hit : response.getHits()) {
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
        );
    }

    private static double distance(double lat1, double lon1, double lat2, double lon2) {
        return SloppyMath.haversinMeters(lat1, lon1, lat2, lon2);
    }
}
