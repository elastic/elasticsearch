/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.relevancesearch.xsearch.action.XSearchAction;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class XSearchRequestValidationServiceTests extends ESTestCase {

    private static ThreadPool threadPool;

    private static XSearchRequestValidationService xSearchRequestValidationService;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("XSearchRequestValidationServiceTests");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        IndexNameExpressionResolver indexNameExpressionResolver = new MockResolver();
        xSearchRequestValidationService = new XSearchRequestValidationService(indexNameExpressionResolver, clusterService);
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testSuccessfulValidationAgainstSingleEngine() {
        XSearchAction.Request validRequest = new XSearchAction.Request(new String[] { "engine1" }, "query", true);
        xSearchRequestValidationService.validateRequest(validRequest);
    }

    public void testSuccessfulValidationAgainstMultipleEngines() {
        XSearchAction.Request validRequest = new XSearchAction.Request(new String[] { "engine1", "engine2" }, "query", true);
        xSearchRequestValidationService.validateRequest(validRequest);
    }

    public void testValidationErrorOnNonEngines() {
        XSearchAction.Request invalidRequest = new XSearchAction.Request(new String[] { "index1", "alias1" }, "query", true);

        ValidationException ex = expectThrows(
            ValidationException.class,
            () -> xSearchRequestValidationService.validateRequest(invalidRequest)
        );
        assertEquals("Validation Failed: 1: XSearch not supported for non-engine indices index1,alias1;", ex.getMessage());
    }

    public void testValidationErrorOnIncompleteEngines() {
        XSearchAction.Request invalidRequest = new XSearchAction.Request(new String[] { "index1", "engine1" }, "query", true);
        ValidationException ex = expectThrows(
            ValidationException.class,
            () -> xSearchRequestValidationService.validateRequest(invalidRequest)
        );
        assertEquals("Validation Failed: 1: XSearch not supported for non-engine indices index1;", ex.getMessage());
    }

    static class MockResolver extends IndexNameExpressionResolver {

        MockResolver() {
            super(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE);
        }

        @Override
        public List<String> searchEngineNames(ClusterState state, IndicesOptions options, String... indexExpressions) {
            return List.of("engine1", "engine2");
        }
    }
}
