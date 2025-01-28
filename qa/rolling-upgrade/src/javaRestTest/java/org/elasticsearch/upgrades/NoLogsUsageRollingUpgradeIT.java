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
import static org.elasticsearch.upgrades.LogsUsageRollingUpgradeIT.rolloverDataStream;
import static org.elasticsearch.upgrades.LogsdbIndexingRollingUpgradeIT.bulkIndex;
import static org.hamcrest.Matchers.hasEntry;

public class NoLogsUsageRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    public NoLogsUsageRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testUsage() throws Exception {
        String dataStreamName = "logs-mysql-error";
        if (isOldCluster()) {
            dataStreamName = dataStreamName.replace("logs-", "log-");
            bulkIndex(dataStreamName, 4, 256, Instant.now());
            ensureGreen(dataStreamName);
        } else {
            if (isFirstMixedCluster()) {

            }
            String newIndex = rolloverDataStream(dataStreamName);
            bulkIndex(dataStreamName, 4, 256, Instant.now());
            Map<?, ?> indexResponse = (Map<?, ?>) getIndexSettings(newIndex, true).get(newIndex);
            Map<?, ?> settings = (Map<?, ?>) indexResponse.get("settings");
            Map<?, ?> defaults = (Map<?, ?>) indexResponse.get("defaults");
            assertThat(settings, hasEntry("index.mode", "logsdb"));
            assertThat(defaults, hasEntry("index.mode", "logsdb"));
            if (isUpgradedCluster()) {
                var response = getClusterSettings();
                assertThat(response, hasEntry("persistent.logsdb.prior_logs_usage", true));
            }
        }
    }

}
