/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.upgrades.LogsdbIndexingRollingUpgradeIT.bulkIndex;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class LogsUsageRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    public LogsUsageRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testUsage() throws Exception {
        assumeFalse("logsdb.prior_logs_usage only gets set in 8.x", oldClusterHasFeature("gte_v9.0.0"));
        String dataStreamName = "logs-mysql-error";
        if (isOldCluster()) {
            bulkIndex(dataStreamName, 4, 256, Instant.now());
            ensureGreen(dataStreamName);
            assertBusy(() -> {
                var getClusterSettingsResponse = getClusterSettings();
                Map<?, ?> persistentSettings = (Map<?, ?>) getClusterSettingsResponse.get("persistent");
                assertThat(persistentSettings, hasEntry("logsdb.prior_logs_usage", "true"));
            }, 2, TimeUnit.MINUTES);
        } else {
            String newIndex = rolloverDataStream(dataStreamName);
            bulkIndex(dataStreamName, 4, 256, Instant.now());
            Map<?, ?> indexResponse = (Map<?, ?>) getIndexSettings(newIndex, true).get(newIndex);
            Map<?, ?> settings = (Map<?, ?>) indexResponse.get("settings");
            Map<?, ?> defaults = (Map<?, ?>) indexResponse.get("defaults");
            assertThat(settings, not(hasKey("index.mode")));
            assertThat(defaults, hasEntry("index.mode", "standard"));
        }
    }

    static Map<String, Object> getClusterSettings() throws IOException {
        var request = new Request("GET", "/_cluster/settings");
        request.addParameter("flat_settings", "true");
        request.addParameter("include_defaults", "true");
        var response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }

    static String rolloverDataStream(String dataStreamName) throws IOException {
        var request = new Request("POST", "/" + dataStreamName + "/_rollover");
        var response = client().performRequest(request);
        assertOK(response);
        var responseBody = entityAsMap(response);
        return (String) responseBody.get("new_index");
    }

}
