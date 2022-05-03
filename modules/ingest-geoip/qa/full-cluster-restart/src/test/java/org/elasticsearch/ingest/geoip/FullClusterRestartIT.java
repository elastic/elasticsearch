/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest.geoip;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.upgrades.AbstractFullClusterRestartTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;

public class FullClusterRestartIT extends AbstractFullClusterRestartTestCase {

    public void testGeoIpSystemFeaturesMigration() throws Exception {
        if (isRunningAgainstOldCluster()) {
            Request enableDownloader = new Request("PUT", "/_cluster/settings");
            enableDownloader.setJsonEntity("""
                {"persistent": {"ingest.geoip.downloader.enabled": true}}
                """);
            assertOK(client().performRequest(enableDownloader));

            Request putPipeline = new Request("PUT", "/_ingest/pipeline/geoip");
            putPipeline.setJsonEntity("""
                {
                    "description": "Add geoip info",
                    "processors": [{
                        "geoip": {
                            "field": "ip",
                            "target_field": "geo",
                            "database_file": "GeoLite2-Country.mmdb"
                        }
                    }]
                }
                """);
            assertOK(client().performRequest(putPipeline));

            // wait for the geo databases to all be loaded
            assertBusy(() -> testDatabasesLoaded(), 30, TimeUnit.SECONDS);

            // the geoip index should be created
            assertBusy(() -> testCatIndices(".geoip_databases"));
            assertBusy(() -> testIndexGeoDoc());
        } else {
            Request migrateSystemFeatures = new Request("POST", "/_migration/system_features");
            assertOK(client().performRequest(migrateSystemFeatures));

            assertBusy(() -> testCatIndices(".geoip_databases-reindexed-for-8", "my-index-00001"));
            assertBusy(() -> testIndexGeoDoc());

            Request disableDownloader = new Request("PUT", "/_cluster/settings");
            disableDownloader.setJsonEntity("""
                {"persistent": {"ingest.geoip.downloader.enabled": false}}
                """);
            assertOK(client().performRequest(disableDownloader));

            // the geoip index should be deleted
            assertBusy(() -> testCatIndices("my-index-00001"));

            Request enableDownloader = new Request("PUT", "/_cluster/settings");
            enableDownloader.setJsonEntity("""
                {"persistent": {"ingest.geoip.downloader.enabled": true}}
                """);
            assertOK(client().performRequest(enableDownloader));

            // wait for the geo databases to all be loaded
            assertBusy(() -> testDatabasesLoaded(), 30, TimeUnit.SECONDS);

            // the geoip index should be recreated
            assertBusy(() -> testCatIndices(".geoip_databases", "my-index-00001"));
            assertBusy(() -> testIndexGeoDoc());
        }
    }

    private void testDatabasesLoaded() throws IOException {
        Request getTaskState = new Request("GET", "/_cluster/state");
        ObjectPath state = ObjectPath.createFromResponse(client().performRequest(getTaskState));

        Map<String, Object> databases = null;
        try {
            databases = state.evaluate("metadata.persistent_tasks.tasks.0.task.geoip-downloader.state.databases");
        } catch (Exception e) {
            // ObjectPath doesn't like the 0 above if the list of tasks is empty, and it throws rather than returning null,
            // catch that and throw an AssertionError instead (which assertBusy will handle)
            fail();
        }
        assertNotNull(databases);

        for (String name : List.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb")) {
            Object database = databases.get(name);
            assertNotNull(database);
            assertNotNull(ObjectPath.evaluate(database, "md5"));
        }
    }

    private void testCatIndices(String... indexNames) throws IOException {
        Request catIndices = new Request("GET", "_cat/indices/*?s=index&h=index&expand_wildcards=all");
        String response = EntityUtils.toString(client().performRequest(catIndices).getEntity());
        List<String> indices = List.of(response.trim().split("\\s+"));
        assertThat(indices, contains(indexNames));
    }

    private void testIndexGeoDoc() throws IOException {
        Request putDoc = new Request("PUT", "/my-index-00001/_doc/my_id?pipeline=geoip");
        putDoc.setJsonEntity("""
            {"ip": "89.160.20.128"}
            """);
        assertOK(client().performRequest(putDoc));

        Request getDoc = new Request("GET", "/my-index-00001/_doc/my_id");
        ObjectPath doc = ObjectPath.createFromResponse(client().performRequest(getDoc));
        assertNull(doc.evaluate("_source.tags"));
        assertEquals("Sweden", doc.evaluate("_source.geo.country_name"));
    }
}
