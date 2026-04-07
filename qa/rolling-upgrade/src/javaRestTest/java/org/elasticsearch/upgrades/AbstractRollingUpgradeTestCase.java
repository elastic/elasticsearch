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
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public abstract class AbstractRollingUpgradeTestCase extends ParameterizedRollingUpgradeTestCase {

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = buildCluster();

    private static ElasticsearchCluster buildCluster() {
        var cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(getOldClusterVersion(), isOldClusterDetachedVersion())
            .nodes(NODE_NUM)
            .setting("path.repo", new Supplier<>() {
                @Override
                @SuppressForbidden(reason = "TemporaryFolder only has io.File methods, not nio.File")
                public String get() {
                    return repoDirectory.getRoot().getPath();
                }
            })
            .setting("xpack.security.enabled", "false")
            .feature(FeatureFlag.TIME_SERIES_MODE);

        // Avoid triggering bogus assertion when serialized parsed mappings don't match with original mappings, because _source key is
        // inconsistent. As usual, we operate under the premise that "versionless" clusters (serverless) are on the latest code and
        // do not need this.
        if (Version.tryParse(getOldClusterVersion()).map(v -> v.before(Version.fromString("8.18.0"))).orElse(false)) {
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }
        return cluster.build();
    }

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    protected AbstractRollingUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    @After
    public void verifyHealthTaskAfterUpgrade() throws IOException {
        if (isUpgradedCluster()) {
            assertHealthNodeTaskAssigned();
            assertHealthReportAvailable();
        }
    }

    private void assertHealthNodeTaskAssigned() throws IOException {
        final var response = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_cluster/state/metadata")));
        final var persistentTasks = response.evaluate("metadata.persistent_tasks");
        final List<?> tasks = response.evaluate("metadata.persistent_tasks.tasks");
        assertThat("tasks cannot be null in metadata.persistent_tasks [" + persistentTasks + "]", tasks, notNullValue());
        String executorNode = null;
        for (int i = 0; i < tasks.size(); i++) {
            String taskId = response.evaluate("metadata.persistent_tasks.tasks." + i + ".id");
            if (HealthNode.TASK_NAME.equals(taskId)) {
                executorNode = response.evaluate("metadata.persistent_tasks.tasks." + i + ".assignment.executor_node");
                break;
            }
        }
        assertThat("health task in metadata.persistent_tasks [" + persistentTasks + "] must be assigned", executorNode, notNullValue());
    }

    private void assertHealthReportAvailable() throws IOException {
        final var healthReport = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_health_report")));
        final var status = healthReport.evaluate("status");
        assertThat("unexpected health report status", status, oneOf("green", "yellow", "red", "unknown"));
    }
}
