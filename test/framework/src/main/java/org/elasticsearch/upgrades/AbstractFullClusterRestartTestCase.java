/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractFullClusterRestartTestCase extends ESRestTestCase {

    private static final boolean runningAgainstOldCluster = Booleans.parseBoolean(System.getProperty("tests.is_old_cluster"));

    @Before
    public void init() throws IOException {
        assertThat(
            "we don't need this branch if we aren't compatible with 6.0",
            Version.CURRENT.minimumIndexCompatibilityVersion().onOrBefore(Version.V_6_0_0),
            equalTo(true)
        );
        if (isRunningAgainstOldCluster() && getOldClusterVersion().before(Version.V_7_0_0)) {
            XContentBuilder template = jsonBuilder();
            template.startObject();
            {
                template.field("index_patterns", "*");
                template.field("order", "0");
                template.startObject("settings");
                template.field("number_of_shards", 5);
                template.endObject();
            }
            template.endObject();
            Request createTemplate = new Request("PUT", "/_template/template");
            createTemplate.setJsonEntity(Strings.toString(template));
            client().performRequest(createTemplate);
        }
    }

    public static boolean isRunningAgainstOldCluster() {
        return runningAgainstOldCluster;
    }

    private static final Version oldClusterVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    /**
     * @return true if test is running against an old cluster before that last major, in this case
     * when System.getProperty("tests.is_old_cluster" == true) and oldClusterVersion is before {@link Version#V_7_0_0}
     */
    protected final boolean isRunningAgainstAncientCluster() {
        return isRunningAgainstOldCluster() && oldClusterVersion.before(Version.V_7_0_0);
    }

    public static Version getOldClusterVersion() {
        return oldClusterVersion;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveClusterSettings() {
        return true;
    }

    @Override
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSLMPoliciesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    protected static void assertNoFailures(Map<?, ?> response) {
        int failed = (int) XContentMapValues.extractValue("_shards.failed", response);
        assertEquals(0, failed);
    }

    protected void assertTotalHits(int expectedTotalHits, Map<?, ?> response) {
        int actualTotalHits = extractTotalHits(response);
        assertEquals(response.toString(), expectedTotalHits, actualTotalHits);
    }

    protected static int extractTotalHits(Map<?, ?> response) {
        if (isRunningAgainstOldCluster() && getOldClusterVersion().before(Version.V_7_0_0)) {
            return (Integer) XContentMapValues.extractValue("hits.total", response);
        } else {
            return (Integer) XContentMapValues.extractValue("hits.total.value", response);
        }
    }
}
