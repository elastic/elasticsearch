/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.eql.action.AsyncEqlSearchResponse;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_EQL_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.eql.action.AsyncEqlSearchResponseTests.assertEqualResponses;
import static org.elasticsearch.xpack.eql.action.EqlSearchResponseTests.randomEqlSearchResponse;
import static org.elasticsearch.xpack.eql.action.AsyncEqlSearchResponseTests.randomAsyncEqlSearchResponse;
import static org.elasticsearch.xpack.eql.action.GetAsyncEqlSearchRequestTests.randomSearchId;

public class AsyncEqlSearchIndexServiceTests extends ESSingleNodeTestCase {
    private AsyncTaskIndexService<AsyncEqlSearchResponse> indexService;

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        indexService = new AsyncTaskIndexService<>(EqlPlugin.INDEX, clusterService, transportService.getThreadPool().getThreadContext(),
            client(), ASYNC_EQL_SEARCH_ORIGIN, AsyncEqlSearchResponse::new, writableRegistry());
    }

    public void testEncodeSearchResponse() throws IOException {
        for (int i = 0; i < 10; i++) {
            AsyncEqlSearchResponse response = randomAsyncEqlSearchResponse(randomSearchId(), randomEqlSearchResponse());
            String encoded = indexService.encodeResponse(response);
            AsyncEqlSearchResponse same = indexService.decodeResponse(encoded);
            assertEqualResponses(response, same);
        }
    }
}
