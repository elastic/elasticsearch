/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.geoip;

import fixture.geoip.GeoIpHttpFixture;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.util.EntityUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.upgrades.ParameterizedFullClusterRestartTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;

@UpdateForV9(owner = UpdateForV9.Owner.DATA_MANAGEMENT)
@LuceneTestCase.AwaitsFix(bugUrl = "we need to figure out the index migrations here for 9.0")
public class FullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    private static final boolean useFixture = Boolean.getBoolean("geoip_use_service") == false;

    private static GeoIpHttpFixture fixture = new GeoIpHttpFixture(useFixture);

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(2)
        .setting("indices.memory.shard_inactive_time", "60m")
        .setting("xpack.security.enabled", "false")
        .setting("ingest.geoip.downloader.endpoint", () -> fixture.getAddress(), s -> useFixture)
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(cluster);

    public FullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

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

    @SuppressWarnings("unchecked")
    private void testDatabasesLoaded() throws IOException {
        Request getTaskState = new Request("GET", "/_cluster/state");
        ObjectPath state = ObjectPath.createFromResponse(client().performRequest(getTaskState));

        List<?> tasks = state.evaluate("metadata.persistent_tasks.tasks");
        // Short-circuit to avoid using steams if the list is empty
        if (tasks.isEmpty()) {
            fail();
        }
        Map<String, Object> databases = (Map<String, Object>) tasks.stream().map(task -> {
            try {
                return ObjectPath.evaluate(task, "task.geoip-downloader.state.databases");
            } catch (IOException e) {
                return null;
            }
        }).filter(Objects::nonNull).findFirst().orElse(null);

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
