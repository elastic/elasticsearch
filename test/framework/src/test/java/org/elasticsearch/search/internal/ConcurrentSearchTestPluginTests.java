/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ConcurrentSearchTestPluginTests extends ESIntegTestCase {

    private final boolean concurrentSearch = randomBoolean();

    public void testConcurrentSearch() {
        client().admin().indices().prepareCreate("index").get();
        ClusterService clusterService = internalCluster().getDataNodeInstance(ClusterService.class);
        int minDocsPerSlice = SearchService.MINIMUM_DOCS_PER_SLICE.get(clusterService.getSettings());
        if (concurrentSearch) {
            assertEquals(1, minDocsPerSlice);
        } else {
            assertEquals(50_000, minDocsPerSlice);
        }
    }

    @Override
    protected boolean enableConcurrentSearch() {
        return concurrentSearch;
    }
}
