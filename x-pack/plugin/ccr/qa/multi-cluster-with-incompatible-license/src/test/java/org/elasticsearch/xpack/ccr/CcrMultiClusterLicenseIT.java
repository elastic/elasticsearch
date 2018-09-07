/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
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
            assertLicenseIncompatible(request);
        }
    }

    public void testCreateAndFollowIndex() {
        if (runningAgainstLeaderCluster == false) {
            final Request request = new Request("POST", "/follower/_ccr/create_and_follow");
            request.setJsonEntity("{\"leader_index\": \"leader_cluster:leader\"}");
            assertLicenseIncompatible(request);
        }
    }

    private static void assertLicenseIncompatible(final Request request) {
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
