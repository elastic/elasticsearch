/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class FieldCapsIT extends AbstractRollingTestCase {
    public void testFieldCaps() throws Exception {
        final String redMapping = """
             "properties": {
               "red_field": { "type": "keyword" },
               "yellow_field": { "type": "integer" },
               "timestamp": {"type": "date"}
             }
            """;
        final String greenMapping = """
             "properties": {
               "green_field": { "type": "keyword" },
               "yellow_field": { "type": "long" },
               "timestamp": {"type": "date"}
             }
            """;
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createIndex("old_red_1", Settings.EMPTY, redMapping);
            createIndex("old_red_2", Settings.EMPTY, redMapping);
            createIndex("old_red_empty", Settings.EMPTY, redMapping);
            createIndex("old_green_1", Settings.EMPTY, greenMapping);
            createIndex("old_green_2", Settings.EMPTY, greenMapping);
            createIndex("old_green_empty", Settings.EMPTY, greenMapping);
            for (String index : List.of("old_red_1", "old_red_2", "old_green_1", "old_green_2")) {
                final Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/1");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity(
                    Strings.toString(JsonXContent.contentBuilder().startObject().field("timestamp", "2020-01-01").endObject())
                );
                assertOK(client().performRequest(indexRequest));
            }
        } else if (CLUSTER_TYPE == ClusterType.MIXED && FIRST_MIXED_ROUND) {
            createIndex("new_red_1", Settings.EMPTY, redMapping);
            createIndex("new_red_2", Settings.EMPTY, redMapping);
            createIndex("new_red_empty", Settings.EMPTY, redMapping);
            createIndex("new_green_1", Settings.EMPTY, greenMapping);
            createIndex("new_green_2", Settings.EMPTY, greenMapping);
            createIndex("new_green_empty", Settings.EMPTY, greenMapping);
            for (String index : List.of("new_red_1", "new_red_2", "new_green_1", "new_green_2")) {
                final Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/1");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity(
                    Strings.toString(JsonXContent.contentBuilder().startObject().field("timestamp", "2020-10-10").endObject())
                );
                assertOK(client().performRequest(indexRequest));
            }
        }
        final QueryBuilder indexFilter = QueryBuilders.rangeQuery("timestamp").gte("2020-01-01").lte("2020-12-12");

        // old red indices without filter
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("old_red_*"), List.of("*"), null);
            assertThat(resp.getIndices(), equalTo(new String[] { "old_red_1", "old_red_2", "old_red_empty" }));
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
        }
        // old red indices with filter
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("old_red_*"), List.of("*"), indexFilter);
            assertThat(resp.getIndices(), equalTo(new String[] { "old_red_1", "old_red_2" }));
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
        }
        if (CLUSTER_TYPE == ClusterType.OLD) {
            // below tests for mixed and upgraded clusters.
            return;
        }
        // all red indices without filter
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("old_red_*", "new_red_*"), List.of("*"), null);
            assertThat(
                resp.getIndices(),
                equalTo(new String[] { "new_red_1", "new_red_2", "new_red_empty", "old_red_1", "old_red_2", "old_red_empty" })
            );
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
        }
        // all red indices with filter
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("old_red_*", "new_red_*"), List.of("*"), indexFilter);
            assertThat(resp.getIndices(), equalTo(new String[] { "new_red_1", "new_red_2", "old_red_1", "old_red_2" }));
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
        }
        // all indices without filter
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("old_*", "new_*"), List.of("*"), null);
            assertThat(
                resp.getIndices(),
                equalTo(
                    new String[] {
                        "new_green_1",
                        "new_green_2",
                        "new_green_empty",
                        "new_red_1",
                        "new_red_2",
                        "new_red_empty",
                        "old_green_1",
                        "old_green_2",
                        "old_green_empty",
                        "old_red_1",
                        "old_red_2",
                        "old_red_empty" }
                )
            );
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("green_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("green_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer", "long"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
            assertTrue(resp.getField("yellow_field").get("long").isSearchable());
        }
        // all indices with filter
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("old_*", "new_*"), List.of("*"), indexFilter);
            assertThat(
                resp.getIndices(),
                equalTo(
                    new String[] {
                        "new_green_1",
                        "new_green_2",
                        "new_red_1",
                        "new_red_2",
                        "old_green_1",
                        "old_green_2",
                        "old_red_1",
                        "old_red_2" }
                )
            );
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("green_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("green_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer", "long"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
            assertTrue(resp.getField("yellow_field").get("long").isSearchable());
        }
    }

}
