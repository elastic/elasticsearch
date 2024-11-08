/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.client.internal.ElasticsearchClient;

import static org.elasticsearch.test.ESTestCase.TEST_REQUEST_TIMEOUT;
import static org.elasticsearch.test.ESTestCase.safeGet;

public class ClusterAllocationExplanationUtils {
    private ClusterAllocationExplanationUtils() {/* no instances */}

    public static ClusterAllocationExplanation getClusterAllocationExplanation(
        ElasticsearchClient client,
        String index,
        int shard,
        boolean primary
    ) {
        return safeGet(
            client.execute(
                TransportClusterAllocationExplainAction.TYPE,
                new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT, index, shard, primary, null)
            )
        ).getExplanation();
    }
}
