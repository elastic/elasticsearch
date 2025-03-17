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

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.upgrades.ParameterizedFullClusterRestartTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class FullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    private static final boolean useFixture = Boolean.getBoolean("geoip_use_service") == false;

    private static final GeoIpHttpFixture fixture = new GeoIpHttpFixture(useFixture);

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(Version.fromString(OLD_CLUSTER_VERSION))
        .nodes(2)
        .setting("ingest.geoip.downloader.endpoint", () -> fixture.getAddress(), s -> useFixture)
        .setting("xpack.security.enabled", "false")
        // .setting("logger.org.elasticsearch.ingest.geoip", "TRACE")
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

    @SuppressWarnings("unchecked")
    public void testGeoIpDatabaseConfigurations() throws Exception {
        if (isRunningAgainstOldCluster()) {
            Request putConfiguration = new Request("PUT", "_ingest/ip_location/database/my-database-1");
            putConfiguration.setJsonEntity("""
                {
                  "name": "GeoIP2-Domain",
                  "maxmind": {
                    "account_id": "1234567"
                  }
                }
                """);
            assertOK(client().performRequest(putConfiguration));
        }

        assertBusy(() -> {
            Request getConfiguration = new Request("GET", "_ingest/ip_location/database/my-database-1");
            Response response = assertOK(client().performRequest(getConfiguration));
            Map<String, Object> map = responseAsMap(response);
            assertThat(map.keySet(), equalTo(Set.of("databases")));
            List<Map<String, Object>> databases = (List<Map<String, Object>>) map.get("databases");
            assertThat(databases, hasSize(1));
            Map<String, Object> database = databases.get(0);
            assertThat(database.get("id"), is("my-database-1"));
            assertThat(database.get("version"), is(1));
            assertThat(database.get("database"), equalTo(Map.of("name", "GeoIP2-Domain", "maxmind", Map.of("account_id", "1234567"))));
        }, 30, TimeUnit.SECONDS);
    }
}
