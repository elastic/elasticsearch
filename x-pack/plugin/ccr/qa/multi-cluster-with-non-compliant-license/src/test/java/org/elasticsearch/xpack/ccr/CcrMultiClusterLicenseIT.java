/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class CcrMultiClusterLicenseIT extends ESRestTestCase {

    private final boolean runningAgainstLeaderCluster = Booleans.parseBoolean(System.getProperty("tests.is_leader_cluster"));

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testFollowIndex() {
        if (runningAgainstLeaderCluster == false) {
            final Request request = new Request("POST", "/follower/_ccr/follow");
            request.setJsonEntity("{\"leader_index\": \"leader_cluster:leader\"}");
            assertNonCompliantLicense(request);
        }
    }

    public void testCreateAndFollowIndex() {
        if (runningAgainstLeaderCluster == false) {
            final Request request = new Request("POST", "/follower/_ccr/create_and_follow");
            request.setJsonEntity("{\"leader_index\": \"leader_cluster:leader\"}");
            assertNonCompliantLicense(request);
        }
    }

    public void testAutoFollow() throws Exception {
        if (runningAgainstLeaderCluster == false) {
            final Request request = new Request("PUT", "/_ccr/auto_follow/leader_cluster");
            request.setJsonEntity("{\"leader_index_patterns\":[\"*\"]}");
            client().performRequest(request);

            // parse the logs and ensure that the auto-coordinator skipped coordination on the leader cluster
            assertBusy(() -> {
                final List<String> lines = Files.readAllLines(PathUtils.get(System.getProperty("log")));

                final Iterator<String> it = lines.iterator();

                boolean warn = false;
                while (it.hasNext()) {
                    final String line = it.next();
                    if (line.matches(".*\\[WARN\\s*\\]\\[o\\.e\\.x\\.c\\.a\\.AutoFollowCoordinator\\s*\\] \\[node-0\\] " +
                            "failure occurred during auto-follower coordination")) {
                        warn = true;
                        break;
                    }
                }
                assertTrue(warn);
                assertTrue(it.hasNext());
                final String lineAfterWarn = it.next();
                assertThat(
                        lineAfterWarn,
                        equalTo("org.elasticsearch.ElasticsearchStatusException: " +
                                "can not fetch remote cluster state as the remote cluster [leader_cluster] is not licensed for [ccr]; " +
                                "the license mode [BASIC] on cluster [leader_cluster] does not enable [ccr]"));
            });
        }
    }

    private static void assertNonCompliantLicense(final Request request) {
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        final String expected = String.format(
                Locale.ROOT,
                "can not fetch remote index [%s] metadata as the remote cluster [%s] is not licensed for [ccr]; " +
                        "the license mode [BASIC] on cluster [%s] does not enable [ccr]",
                "leader_cluster:leader",
                "leader_cluster",
                "leader_cluster");
        assertThat(e, hasToString(containsString(expected)));
    }

}
