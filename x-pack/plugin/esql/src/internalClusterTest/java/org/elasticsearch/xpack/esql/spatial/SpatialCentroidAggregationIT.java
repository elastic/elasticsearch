/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SpatialCentroidAggregationIT extends SpatialCentroidAggregationTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SpatialPlugin.class, EsqlPluginWithEnterpriseOrTrialLicense.class);
    }

    @Override
    public void testStCentroidAggregationWithShapes() {
        assertStCentroidFromIndex("index_geo_shape", 1e-7);
    }

    public void testStCentroidAggregationByGroupManyGroupsRegression() {
        String index = "test_geo_bug";
        assertAcked(prepareCreate(index).setMapping("""
            {
              "properties": {
                "group_id": { "type": "integer" },
                "location": { "type": "geo_point" }
              }
            }
            """));
        var bulk = client().prepareBulk();
        for (int i = 1; i <= 136; i++) {
            IndexRequest req = new IndexRequest(index).id(Integer.toString(i)).source("group_id", i);
            if (i <= 129) {
                req.source("group_id", i, "location", "39.9,116.3");
            }
            bulk.add(req);
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow(index);

        String query = String.format(Locale.ROOT, """
            FROM %s
            | STATS centroid = ST_CENTROID_AGG(location) BY group_id
            | SORT group_id ASC
            | EVAL x = ST_X(centroid), y = ST_Y(centroid)
            """, index);
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("centroid", "group_id", "x", "y"));
            assertColumnTypes(resp.columns(), List.of("geo_point", "integer", "double", "double"));
            List<List<Object>> values = getValuesList(resp.values());
            assertThat(values.size(), equalTo(136));
            for (int i = 0; i < values.size(); i++) {
                int expectedGroup = i + 1;
                List<Object> row = values.get(i);
                assertThat((Integer) row.get(1), equalTo(expectedGroup));
                if (expectedGroup <= 129) {
                    assertThat((Double) row.get(2), closeTo(116.3, 1e-7));
                    assertThat((Double) row.get(3), closeTo(39.9, 1e-7));
                } else {
                    assertThat(row.get(2), nullValue());
                    assertThat(row.get(3), nullValue());
                }
            }
        }
    }
}
