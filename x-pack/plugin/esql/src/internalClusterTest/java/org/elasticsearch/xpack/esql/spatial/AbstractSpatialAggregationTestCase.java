/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;

import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * This class provides common index and data setup for spatial aggregations, with both points and shapes
 * evenly distributed between -10 and 10, so that both extents and centroid aggregations can get
 * deterministic results.
 */
public abstract class AbstractSpatialAggregationTestCase extends AbstractEsqlIntegTestCase {

    protected void createAndPopulateIndexes(double minX, double maxX, double minY, double maxY) {
        int numX = 21;
        int numY = 21;
        double stepX = (maxX - minX) / (numX - 1);
        double stepY = (maxY - minY) / (numY - 1);
        initIndex("index_", "geo_point");
        initIndex("index_", "geo_shape");
        BulkRequestBuilder points = client().prepareBulk();
        BulkRequestBuilder shapes = client().prepareBulk();
        for (int xi = 0; xi < numX; xi++) {
            for (int yi = 0; yi < numY; yi++) {
                double x = minX + xi * stepX;
                double y = minY + yi * stepY;
                String point = "POINT(" + x + " " + y + ")";
                points.add(new IndexRequest("index_geo_point").id(x + ":" + y).source("location", point));
                if (xi > 0 && yi > 0) {
                    double px = minX + (xi - 1) * stepX;
                    double py = minY + (yi - 1) * stepY;
                    String shape = "BBOX(" + px + ", " + x + ", " + y + ", " + py + ")";
                    shapes.add(new IndexRequest("index_geo_shape").id(x + ":" + y).source("location", shape));
                }
            }
        }
        points.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        shapes.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow("index_geo_point");
        ensureYellow("index_geo_shape");
    }

    protected void initIndex(String prefix, String fieldType) {
        assertAcked(prepareCreate(prefix + fieldType).setMapping(String.format(Locale.ROOT, """
            {
              "properties" : {
                "location": { "type" : "%s" }
              }
            }
            """, fieldType)));
    }
}
