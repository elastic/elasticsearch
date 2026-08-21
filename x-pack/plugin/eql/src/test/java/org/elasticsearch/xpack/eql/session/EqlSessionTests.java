/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.eql.EqlTestUtils;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.util.DateUtils;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class EqlSessionTests extends ESTestCase {

    /**
     * The engine self-defends: pre-resolved field-caps supplied by a caller are reused only when the request has no
     * runtime mappings — those change the mapping the engine must plan against, so a request that defines them always
     * self-resolves, regardless of what the caller supplied.
     */
    public void testRuntimeMappingsDisableFieldCapsReuse() {
        try (var threadPool = createThreadPool()) {
            LogicalPlan plan = new EqlParser().createStatement("process where true");
            FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(new String[] { "idx" }, Map.of());

            // caps + no runtime mappings -> reuse: the mapping is built from the response via the resolver's local
            // merge overload (a pure merge, no _field_caps), never the network resolveAsMergedMapping path.
            IndexResolver reuseResolver = mock(IndexResolver.class);
            session(threadPool, reuseResolver, configuration(caps, emptyMap())).analyzedPlan(plan, ActionListener.noop());
            verify(reuseResolver).mergedMappings(anyString(), any());
            verify(reuseResolver, never()).resolveAsMergedMapping(anyString(), any(), any(), any(), anyBoolean(), any(), any());

            // caps + runtime mappings -> the engine self-defends and self-resolves through the resolver instead.
            IndexResolver selfResolver = mock(IndexResolver.class);
            session(threadPool, selfResolver, configuration(caps, singletonMap("rt", Map.of("type", "keyword")))).analyzedPlan(
                plan,
                ActionListener.noop()
            );
            verify(selfResolver).resolveAsMergedMapping(anyString(), any(), any(), any(), anyBoolean(), any(), any());
        }
    }

    private static EqlSession session(ThreadPool threadPool, IndexResolver indexResolver, EqlConfiguration cfg) {
        return new EqlSession(
            new NoOpClient(threadPool),
            cfg,
            indexResolver,
            new PreAnalyzer(),
            new PostAnalyzer(),
            new EqlFunctionRegistry(),
            new Verifier(new Metrics()),
            new Optimizer(),
            new Planner(),
            new NoopCircuitBreaker("test")
        );
    }

    private static EqlConfiguration configuration(FieldCapabilitiesResponse preResolvedFieldCaps, Map<String, Object> runtimeMappings) {
        return new EqlConfiguration(
            new String[] { "idx" },
            new String[] { "idx" },
            DateUtils.UTC,
            "nobody",
            "cluster",
            null,
            runtimeMappings,
            null,
            TimeValue.timeValueSeconds(30),
            null,
            123,
            1,
            false,
            true,
            null,
            "",
            new TaskId("test", 123),
            EqlTestUtils.randomTask(),
            false,
            null,
            preResolvedFieldCaps
        );
    }

    public void testAccumulateDirectoryMetricsSumsAcrossSubSearches() {
        try (var threadPool = createThreadPool()) {
            EqlSession session = newSession(threadPool);
            assertTrue(session.directoryMetrics().isEmpty());

            long expectedBytesRead = 0;
            int subSearches = randomIntBetween(1, 20);
            for (int i = 0; i < subSearches; i++) {
                long bytesRead = randomLongBetween(1, 1000);
                expectedBytesRead += bytesRead;
                session.accumulateDirectoryMetrics(storeMetrics(bytesRead));
            }

            assertEquals(expectedBytesRead, session.directoryMetrics().metrics(StoreMetrics.NAME).cast(StoreMetrics.class).getBytesRead());
        }
    }

    public void testAccumulateDirectoryMetricsSkipsEmptyAndNull() {
        try (var threadPool = createThreadPool()) {
            EqlSession session = newSession(threadPool);
            assertTrue(session.directoryMetrics().isEmpty());

            session.accumulateDirectoryMetrics(DirectoryMetrics.EMPTY);
            assertTrue(session.directoryMetrics().isEmpty());

            session.accumulateDirectoryMetrics(null);
            assertTrue(session.directoryMetrics().isEmpty());
        }
    }

    private static EqlSession newSession(ThreadPool threadPool) {
        return new EqlSession(
            new NoOpClient(threadPool),
            EqlTestUtils.randomConfiguration(),
            null,
            new PreAnalyzer(),
            new PostAnalyzer(),
            new EqlFunctionRegistry(),
            new Verifier(new Metrics()),
            new Optimizer(),
            new Planner(),
            new NoopCircuitBreaker("test")
        );
    }

    private static DirectoryMetrics storeMetrics(long bytesRead) {
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(StoreMetrics.NAME, new StoreMetrics(bytesRead));
        return builder.build();
    }
}
