/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.search.AsyncSearchResponseTests.assertEqualResponses;
import static org.elasticsearch.xpack.search.AsyncSearchResponseTests.randomAsyncSearchResponse;
import static org.elasticsearch.xpack.search.AsyncSearchResponseTests.randomSearchResponse;
import static org.elasticsearch.xpack.search.GetAsyncSearchRequestTests.randomSearchId;

// TODO: test CRUD operations
public class AsyncSearchIndexServiceTests extends ESSingleNodeTestCase {
    private AsyncTaskIndexService indexService;

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        indexService = new AsyncTaskIndexService(AsyncSearch.INDEX, clusterService, transportService.getThreadPool().getThreadContext(),
            client(), ASYNC_SEARCH_ORIGIN, writableRegistry());
    }

    public void testEncodeSearchResponse() throws IOException {
        for (int i = 0; i < 10; i++) {
            AsyncSearchResponse response = randomAsyncSearchResponse(randomSearchId(), randomSearchResponse());
            String encoded = indexService.encodeResponse(response);
            AsyncSearchResponse same = indexService.decodeResponse(encoded);
            assertEqualResponses(response, same);
        }
    }
}
