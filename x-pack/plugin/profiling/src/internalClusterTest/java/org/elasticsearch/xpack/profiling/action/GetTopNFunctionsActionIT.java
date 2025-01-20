/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class GetTopNFunctionsActionIT extends ProfilingTestCase {
    public void testGetTopNFunctionsUnfiltered() throws Exception {
        GetStackTracesRequest request = new GetStackTracesRequest(
            1000,
            600.0d,
            1.0d,
            1.0d,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        request.setAdjustSampleCount(true);
        GetTopNFunctionsResponse response = client().execute(GetTopNFunctionsAction.INSTANCE, request).get();
        assertEquals(747, response.getTopN().size());
    }

    public void testGetTopNFunctionsGroupedByServiceName() throws Exception {
        GetStackTracesRequest request = new GetStackTracesRequest(
            1000,
            600.0d,
            1.0d,
            1.0d,
            null,
            null,
            null,
            new String[] { "service.name" },
            null,
            null,
            null,
            null,
            null
        );
        request.setAdjustSampleCount(true);
        request.setLimit(50);
        GetTopNFunctionsResponse response = client().execute(GetTopNFunctionsAction.INSTANCE, request).get();
        assertEquals(50, response.getTopN().size());
    }

    public void testGetTopNFunctionsFromAPM() throws Exception {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.must().add(QueryBuilders.termQuery("transaction.name", "encodeSha1"));
        query.must().add(QueryBuilders.rangeQuery("@timestamp").lte("1698624000"));

        GetStackTracesRequest request = new GetStackTracesRequest(
            null,
            1.0d,
            1.0d,
            1.0d,
            query,
            // also match an index that does not contain stacktrace ids to ensure it is ignored
            new String[] { "apm-test-*", "apm-legacy-test-*" },
            "transaction.profiler_stack_trace_ids",
            new String[] { "transaction.name" },
            null,
            null,
            null,
            null,
            null
        );
        GetTopNFunctionsResponse response = client().execute(GetTopNFunctionsAction.INSTANCE, request).get();
        assertEquals(45, response.getTopN().size());
    }
}
