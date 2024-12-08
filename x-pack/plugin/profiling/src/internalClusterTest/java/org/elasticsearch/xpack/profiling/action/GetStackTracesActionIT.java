/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.util.List;

public class GetStackTracesActionIT extends ProfilingTestCase {
    public void testGetStackTracesUnfiltered() throws Exception {
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
            null,
            null
        );
        request.setAdjustSampleCount(true);
        GetStackTracesResponse response = client().execute(GetStackTracesAction.INSTANCE, request).get();
        assertEquals(46, response.getTotalSamples());
        assertEquals(1821, response.getTotalFrames());

        assertNotNull(response.getStackTraceEvents());
        assertEquals(3L, response.getStackTraceEvents().get("L7kj7UvlKbT-vN73el4faQ").count);

        assertNotNull(response.getStackTraces());
        // just do a high-level spot check. Decoding is tested in unit-tests
        StackTrace stackTrace = response.getStackTraces().get("L7kj7UvlKbT-vN73el4faQ");
        assertEquals(18, stackTrace.addressOrLines.length);
        assertEquals(18, stackTrace.fileIds.length);
        assertEquals(18, stackTrace.frameIds.length);
        assertEquals(18, stackTrace.typeIds.length);
        assertEquals(0.0000051026469d, stackTrace.annualCO2Tons, 0.0000000001d);
        assertEquals(0.19825d, stackTrace.annualCostsUSD, 0.00001d);
        // not determined by default
        assertNull(stackTrace.subGroups);

        assertNotNull(response.getStackFrames());
        StackFrame stackFrame = response.getStackFrames().get("8NlMClggx8jaziUTJXlmWAAAAAAAAIYI");
        assertEquals(List.of("start_thread"), stackFrame.functionName);

        assertNotNull(response.getExecutables());
        assertEquals("vmlinux", response.getExecutables().get("lHp5_WAgpLy2alrUVab6HA"));
    }

    public void testGetStackTracesGroupedByServiceName() throws Exception {
        GetStackTracesRequest request = new GetStackTracesRequest(
            1000,
            600.0d,
            1.0d,
            1.0d,
            null,
            null,
            null,
            "service.name",
            null,
            null,
            null,
            null,
            null,
            null
        );
        request.setAdjustSampleCount(true);
        GetStackTracesResponse response = client().execute(GetStackTracesAction.INSTANCE, request).get();
        assertEquals(46, response.getTotalSamples());
        assertEquals(1821, response.getTotalFrames());

        assertNotNull(response.getStackTraceEvents());
        assertEquals(3L, response.getStackTraceEvents().get("L7kj7UvlKbT-vN73el4faQ").count);

        assertNotNull(response.getStackTraces());
        // just do a high-level spot check. Decoding is tested in unit-tests
        StackTrace stackTrace = response.getStackTraces().get("L7kj7UvlKbT-vN73el4faQ");
        assertEquals(18, stackTrace.addressOrLines.length);
        assertEquals(18, stackTrace.fileIds.length);
        assertEquals(18, stackTrace.frameIds.length);
        assertEquals(18, stackTrace.typeIds.length);
        assertEquals(0.0000051026469d, stackTrace.annualCO2Tons, 0.0000000001d);
        assertEquals(0.19825d, stackTrace.annualCostsUSD, 0.00001d);
        assertEquals(Long.valueOf(2L), stackTrace.subGroups.getCount("basket"));

        assertNotNull(response.getStackFrames());
        StackFrame stackFrame = response.getStackFrames().get("8NlMClggx8jaziUTJXlmWAAAAAAAAIYI");
        assertEquals(List.of("start_thread"), stackFrame.functionName);

        assertNotNull(response.getExecutables());
        assertEquals("vmlinux", response.getExecutables().get("lHp5_WAgpLy2alrUVab6HA"));
    }

    public void testGetStackTracesFromAPMWithMatchNoDownsampling() throws Exception {
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
            "transaction.name",
            null,
            null,
            null,
            null,
            null,
            null
        );
        GetStackTracesResponse response = client().execute(GetStackTracesAction.INSTANCE, request).get();
        assertEquals(49, response.getTotalFrames());
        assertEquals(1.0d, response.getSamplingRate(), 0.001d);

        assertNotNull(response.getStackTraceEvents());
        assertEquals(3L, response.getStackTraceEvents().get("Ce77w10WeIDow3kd1jowlA").count);
        assertEquals(2L, response.getStackTraceEvents().get("JvISdnJ47BQ01489cwF9DA").count);

        assertNotNull(response.getStackTraces());
        // just do a high-level spot check. Decoding is tested in unit-tests
        StackTrace stackTrace = response.getStackTraces().get("Ce77w10WeIDow3kd1jowlA");
        assertEquals(39, stackTrace.addressOrLines.length);
        assertEquals(39, stackTrace.fileIds.length);
        assertEquals(39, stackTrace.frameIds.length);
        assertEquals(39, stackTrace.typeIds.length);
        assertTrue(stackTrace.annualCO2Tons > 0.0d);
        assertTrue(stackTrace.annualCostsUSD > 0.0d);
        assertEquals(Long.valueOf(3L), stackTrace.subGroups.getCount("encodeSha1"));

        assertNotNull(response.getStackFrames());
        StackFrame stackFrame = response.getStackFrames().get("fhsEKXDuxJ-jIJrZpdRuSAAAAAAAAFtj");
        assertEquals(List.of("deflate", "deflate"), stackFrame.functionName);

        assertNotNull(response.getExecutables());
        assertEquals("libzip.so", response.getExecutables().get("GXH6S9Nv2Lf0omTz4cH4RA"));
    }

    public void testGetStackTracesFromAPMWithMatchAndDownsampling() throws Exception {
        TermQueryBuilder query = QueryBuilders.termQuery("transaction.name", "encodeSha1");

        GetStackTracesRequest request = new GetStackTracesRequest(
            1,
            1.0d,
            1.0d,
            1.0d,
            query,
            new String[] { "apm-test-*" },
            "transaction.profiler_stack_trace_ids",
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        // ensures consistent results in the random sampler aggregation that is used internally
        request.setShardSeed(42);

        GetStackTracesResponse response = client().execute(GetStackTracesAction.INSTANCE, request).get();
        assertEquals(49, response.getTotalFrames());
        assertEquals(0.2d, response.getSamplingRate(), 0.001d);

        assertNotNull(response.getStackTraceEvents());
        // as the sampling rate is 0.2, we see 5 times more samples (random sampler agg automatically adjusts sample count)
        assertEquals(5 * 3L, response.getStackTraceEvents().get("Ce77w10WeIDow3kd1jowlA").count);
        assertEquals(5 * 2L, response.getStackTraceEvents().get("JvISdnJ47BQ01489cwF9DA").count);

        assertNotNull(response.getStackTraces());
        // just do a high-level spot check. Decoding is tested in unit-tests
        StackTrace stackTrace = response.getStackTraces().get("Ce77w10WeIDow3kd1jowlA");
        assertEquals(39, stackTrace.addressOrLines.length);
        assertEquals(39, stackTrace.fileIds.length);
        assertEquals(39, stackTrace.frameIds.length);
        assertEquals(39, stackTrace.typeIds.length);
        assertTrue(stackTrace.annualCO2Tons > 0.0d);
        assertTrue(stackTrace.annualCostsUSD > 0.0d);
        // not determined by default
        assertNull(stackTrace.subGroups);

        assertNotNull(response.getStackFrames());
        StackFrame stackFrame = response.getStackFrames().get("fhsEKXDuxJ-jIJrZpdRuSAAAAAAAAFtj");
        assertEquals(List.of("deflate", "deflate"), stackFrame.functionName);

        assertNotNull(response.getExecutables());
        assertEquals("libzip.so", response.getExecutables().get("GXH6S9Nv2Lf0omTz4cH4RA"));
    }

    public void testGetStackTracesFromAPMNoMatch() throws Exception {
        TermQueryBuilder query = QueryBuilders.termQuery("transaction.name", "nonExistingTransaction");

        GetStackTracesRequest request = new GetStackTracesRequest(
            null,
            1.0d,
            1.0d,
            1.0d,
            query,
            new String[] { "apm-test-*" },
            "transaction.profiler_stack_trace_ids",
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        GetStackTracesResponse response = client().execute(GetStackTracesAction.INSTANCE, request).get();
        assertEquals(0, response.getTotalFrames());
    }

    public void testGetStackTracesFromAPMIndexNotAvailable() throws Exception {
        TermQueryBuilder query = QueryBuilders.termQuery("transaction.name", "nonExistingTransaction");

        GetStackTracesRequest request = new GetStackTracesRequest(
            null,
            1.0d,
            1.0d,
            1.0d,
            query,
            new String[] { "non-existing-apm-index-*" },
            "transaction.profiler_stack_trace_ids",
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        GetStackTracesResponse response = client().execute(GetStackTracesAction.INSTANCE, request).get();
        assertEquals(0, response.getTotalFrames());
    }

    public void testGetStackTracesFromAPMStackTraceFieldNotAvailable() throws Exception {
        TermQueryBuilder query = QueryBuilders.termQuery("transaction.name", "encodeSha1");

        GetStackTracesRequest request = new GetStackTracesRequest(
            null,
            1.0d,
            1.0d,
            1.0d,
            query,
            new String[] { "apm-legacy-test-*" },
            "transaction.profiler_stack_trace_ids",
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        GetStackTracesResponse response = client().execute(GetStackTracesAction.INSTANCE, request).get();
        assertEquals(0, response.getTotalFrames());
    }
}
