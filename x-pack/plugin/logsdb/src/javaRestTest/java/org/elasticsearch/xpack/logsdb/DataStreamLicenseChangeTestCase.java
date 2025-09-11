/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;

import java.io.IOException;

public abstract class DataStreamLicenseChangeTestCase extends LogsIndexModeRestTestIT {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .setting("cluster.logsdb.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "basic")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected static void startBasic() throws IOException {
        Request startTrial = new Request("POST", "/_license/start_basic");
        startTrial.addParameter("acknowledge", "true");
        assertOK(client().performRequest(startTrial));
    }

    protected static void startTrial() throws IOException {
        Request startTrial = new Request("POST", "/_license/start_trial");
        startTrial.addParameter("acknowledge", "true");
        assertOK(client().performRequest(startTrial));
    }

}
