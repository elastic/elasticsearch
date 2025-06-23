/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;

import java.io.IOException;

public class MultiProjectRestartIT extends MultiProjectRestTestCase {

    @ClassRule
    public static ElasticsearchCluster CLUSTER = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return CLUSTER.getHttpAddresses();
    }

    @FixForMultiProject(description = "cannot clean up since clients are closed after restart")
    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testRestartShouldSucceed() throws IOException {
        final String indexName = randomIdentifier();
        final String projectId = randomIdentifier();
        createProject(projectId);
        final Request createIndexRequest = new Request("PUT", "/" + indexName);
        createIndexRequest.setJsonEntity("""
            {
                "settings": {
                  "number_of_shards": 1,
                  "number_of_replicas": 0
                }
            }""");

        setRequestProjectId(createIndexRequest, projectId);
        assertOK(client().performRequest(createIndexRequest));

        CLUSTER.restart(false);

        // Need rebuild client due to port changes
        closeClients();
        @FixForMultiProject(description = "replace with initClient() when it knows how to handle multi-project cluster state")
        var newClient = buildClient(
            Settings.builder().put(super.restClientSettings()).build(),
            parseClusterHosts(getTestRestCluster()).toArray(HttpHost[]::new)
        );
        try (newClient) {
            final Request getIndexRequest = new Request("GET", "/" + indexName);
            setRequestProjectId(getIndexRequest, projectId);
            assertOK(newClient.performRequest(getIndexRequest));
        }
    }
}
