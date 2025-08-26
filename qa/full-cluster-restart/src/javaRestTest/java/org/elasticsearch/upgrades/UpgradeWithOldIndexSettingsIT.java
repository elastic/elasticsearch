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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class UpgradeWithOldIndexSettingsIT extends ParameterizedFullClusterRestartTestCase {

    protected static LocalClusterConfigProvider clusterConfig = c -> {};

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .apply(() -> clusterConfig)
        .build();

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public UpgradeWithOldIndexSettingsIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    public void testMapperDynamicIndexSetting() throws IOException {
        assumeTrue(
            "Setting deprecated in 6.x, but was disallowed/removed incorrectly in some 7.x versions and can only be set safely in 7.17.22. "
                + "Setting can't be used in 8.x ",
            getOldClusterTestVersion().before("8.0.0") && getOldClusterTestVersion().after("7.17.21")
        );
        String indexName = "my-index";
        if (isRunningAgainstOldCluster()) {
            createIndex(indexName);

            var request = new Request("PUT", "/my-index/_settings");
            request.setJsonEntity(org.elasticsearch.common.Strings.toString(Settings.builder().put("index.mapper.dynamic", true).build()));
            request.setOptions(
                expectWarnings(
                    "[index.mapper.dynamic] setting was deprecated in Elasticsearch and will be removed in a future release! "
                        + "See the breaking changes documentation for the next major version."
                )
            );
            assertOK(client().performRequest(request));
        } else {
            var indexSettings = getIndexSettings(indexName);
            assertThat(XContentMapValues.extractValue(indexName + ".settings.index.mapper.dynamic", indexSettings), equalTo("true"));
            ensureGreen(indexName);
            // New indices can never define the index.mapper.dynamic setting.
            Exception e = expectThrows(
                ResponseException.class,
                () -> createIndex("my-index2", Settings.builder().put("index.mapper.dynamic", true).build())
            );
            assertThat(e.getMessage(), containsString("unknown setting [index.mapper.dynamic]"));
        }
    }

}
