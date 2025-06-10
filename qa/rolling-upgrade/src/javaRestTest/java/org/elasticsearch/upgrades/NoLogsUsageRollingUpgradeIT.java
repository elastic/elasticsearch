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

import java.time.Instant;
import java.util.Map;

import static org.elasticsearch.upgrades.LogsUsageRollingUpgradeIT.getClusterSettings;
import static org.elasticsearch.upgrades.LogsdbIndexingRollingUpgradeIT.bulkIndex;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class NoLogsUsageRollingUpgradeIT extends AbstractRollingUpgradeWithSecurityTestCase {

    public NoLogsUsageRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testUsage() throws Exception {
        String dataStreamName = "logs-mysql-error";
        if (isOldCluster()) {
            dataStreamName = dataStreamName.replace("logs-", "log-");
            bulkIndex(dataStreamName, 4, 256, Instant.now());
            ensureGreen(dataStreamName);
        } else if (isUpgradedCluster()) {
            String newIndex = bulkIndex(dataStreamName, 4, 256, Instant.now());
            ensureGreen(dataStreamName);
            Map<?, ?> indexResponse = (Map<?, ?>) getIndexSettings(newIndex, true).get(newIndex);
            Map<?, ?> settings = (Map<?, ?>) indexResponse.get("settings");
            assertThat(settings, hasEntry("index.mode", "logsdb"));
            var getClusterSettingsResponse = getClusterSettings();
            Map<?, ?> defaults = (Map<?, ?>) getClusterSettingsResponse.get("defaults");
            Map<?, ?> persistentSettings = (Map<?, ?>) getClusterSettingsResponse.get("persistent");
            assertThat(persistentSettings, not(hasKey("logsdb.prior_logs_usage")));
            assertThat(defaults, hasEntry("cluster.logsdb.enabled", "true"));
        }
    }

}
