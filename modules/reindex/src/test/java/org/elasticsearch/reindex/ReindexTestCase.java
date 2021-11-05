/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;

/**
 * Base test case for integration tests against the reindex plugin.
 */
@ClusterScope(scope = SUITE)
public abstract class ReindexTestCase extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class);
    }

    protected ReindexRequestBuilder reindex() {
        return new ReindexRequestBuilder(client(), ReindexAction.INSTANCE);
    }

    protected UpdateByQueryRequestBuilder updateByQuery() {
        return new UpdateByQueryRequestBuilder(client(), UpdateByQueryAction.INSTANCE);
    }

    protected DeleteByQueryRequestBuilder deleteByQuery() {
        return new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE);
    }

    protected RethrottleRequestBuilder rethrottle() {
        return new RethrottleRequestBuilder(client(), RethrottleAction.INSTANCE);
    }

    public static BulkIndexByScrollResponseMatcher matcher() {
        return new BulkIndexByScrollResponseMatcher();
    }

    static int randomSlices(int min, int max) {
        if (randomBoolean()) {
            return AbstractBulkByScrollRequest.AUTO_SLICES;
        } else {
            return between(min, max);
        }
    }

    static int randomSlices() {
        return randomSlices(2, 10);
    }

    /**
     * Figures out how many slices the request handling will use
     */
    protected int expectedSlices(int requestSlices, Collection<String> indices) {
        if (requestSlices == AbstractBulkByScrollRequest.AUTO_SLICES) {
            int leastNumShards = Collections.min(
                indices.stream().map(sourceIndex -> getNumShards(sourceIndex).numPrimaries).collect(Collectors.toList())
            );
            return Math.min(leastNumShards, BulkByScrollParallelizationHelper.AUTO_SLICE_CEILING);
        } else {
            return requestSlices;
        }
    }

    protected int expectedSlices(int requestSlices, String index) {
        return expectedSlices(requestSlices, singleton(index));
    }

    /**
     * Figures out how many slice statuses to expect in the response
     */
    protected int expectedSliceStatuses(int requestSlices, Collection<String> indices) {
        int slicesConfigured = expectedSlices(requestSlices, indices);

        if (slicesConfigured > 1) {
            return slicesConfigured;
        } else {
            return 0;
        }
    }

    protected int expectedSliceStatuses(int slicesConfigured, String index) {
        return expectedSliceStatuses(slicesConfigured, singleton(index));
    }
}
