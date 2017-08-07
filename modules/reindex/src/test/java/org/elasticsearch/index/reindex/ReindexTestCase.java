/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

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

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(ReindexPlugin.class);
    }

    protected ReindexRequestBuilder reindex() {
        return ReindexAction.INSTANCE.newRequestBuilder(client());
    }

    protected UpdateByQueryRequestBuilder updateByQuery() {
        return UpdateByQueryAction.INSTANCE.newRequestBuilder(client());
    }

    protected DeleteByQueryRequestBuilder deleteByQuery() {
        return DeleteByQueryAction.INSTANCE.newRequestBuilder(client());
    }

    protected RethrottleRequestBuilder rethrottle() {
        return RethrottleAction.INSTANCE.newRequestBuilder(client());
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
            int leastNumShards = Collections.min(indices.stream()
                .map(sourceIndex -> getNumShards(sourceIndex).numPrimaries)
                .collect(Collectors.toList()));
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
