/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.hamcrest.Matchers;

import java.nio.charset.StandardCharsets;

public class GeoIpUpgradeIT extends AbstractUpgradeTestCase {

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/96129")
    public void testGeoIpDownloader() throws Exception {
        assumeTrue("Disabled until PR #95621 is backported to branch " + Version.V_8_8_0, UPGRADE_FROM_VERSION.onOrBefore(Version.V_8_7_0));

        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            assertBusy(() -> {
                Response response = client().performRequest(new Request("GET", "_cat/tasks"));
                String tasks = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                assertThat(tasks, Matchers.containsString("geoip-downloader"));
            });
            assertBusy(() -> {
                Response response = client().performRequest(new Request("GET", "_ingest/geoip/stats"));
                String tasks = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                // The geoip downloader doesn't actually do anything since there are no geoip processors:
                assertThat(tasks, Matchers.containsString("failed_downloads\":0"));
                assertThat(tasks, Matchers.containsString("successful_downloads\":0"));
            });
        }
    }
}
