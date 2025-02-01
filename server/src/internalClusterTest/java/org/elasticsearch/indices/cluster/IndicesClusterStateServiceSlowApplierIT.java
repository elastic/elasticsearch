/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;

public class IndicesClusterStateServiceSlowApplierIT extends ESIntegTestCase {

    private static final int NUMBER_OF_INDICES = Integer.parseInt(System.getProperty("testDeletions.numberOfIndices", "300"));

    public void testDeleteManyIndices() {
        internalCluster().ensureAtLeastNumDataNodes(3);

        int numberOfIndices = NUMBER_OF_INDICES;
        logger.info("Creating {} indices", numberOfIndices);
        final String indexPrefix = randomIdentifier() + "_";
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = indexPrefix + i;
            createIndex(indexName);
            indexRandom(true, indexName, 20);
            ensureGreen(indexName);
        }

        long startTime = System.currentTimeMillis();
        client().execute(TransportDeleteIndexAction.TYPE, new DeleteIndexRequest("_all")).actionGet();
        logger.info("Deleting {} indices took {}", numberOfIndices, TimeValue.timeValueMillis(System.currentTimeMillis() - startTime));
    }

    @Override
    protected int maximumNumberOfShards() {
        return 1;
    }
}
