/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;

public class TransformFailureHandlerTests extends ESTestCase {

    static class MockTransformContextListener implements TransformContext.Listener {

        private boolean failed = false;
        private int failureCountChangedCounter = 0;

        public void reset() {
            failed = false;
            failureCountChangedCounter = 0;
        }

        @Override
        public void shutdown() {

        }

        @Override
        public void failureCountChanged() {
            failureCountChangedCounter++;
        }

        @Override
        public void fail(Throwable exception, String failureMessage, ActionListener<Void> listener) {
            failed = true;
        }

        public boolean getFailed() {
            return failed;
        }

        public int getFailureCountChangedCounter() {
            return failureCountChangedCounter;
        }
    }

    public void testHandleIndexerFailure_CircuitBreakingExceptionNewPageSizeLessThanMinimumPageSize() {
        var e = new CircuitBreakingException(randomAlphaOfLength(10), 1, 0, randomFrom(CircuitBreaker.Durability.values()));
        assertRetryIfUnattendedOtherwiseFail(e);
    }

    public void testHandleIndexerFailure_CircuitBreakingExceptionNewPageSizeNotLessThanMinimumPageSize() {
        var e = new CircuitBreakingException(randomAlphaOfLength(10), 1, 1, randomFrom(CircuitBreaker.Durability.values()));

        List.of(true, false).forEach((unattended) -> { assertNoFailureAndContextPageSizeSet(e, unattended, 365); });
    }

    public void testHandleIndexerFailure_ScriptException() {
        var e = new ScriptException(
            randomAlphaOfLength(10),
            new ArithmeticException(randomAlphaOfLength(10)),
            singletonList(randomAlphaOfLength(10)),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
        assertRetryIfUnattendedOtherwiseFail(e);
    }

    public void testHandleIndexerFailure_BulkIndexExceptionWrappingClusterBlockException() {
        final BulkIndexingException bulkIndexingException = new BulkIndexingException(
            randomAlphaOfLength(10),
            new ClusterBlockException(Map.of("test-index", Set.of(MetadataIndexStateService.INDEX_CLOSED_BLOCK))),
            randomBoolean()
        );

        List.of(true, false).forEach((unattended) -> { assertClusterBlockHandled(bulkIndexingException, unattended); });
    }

    public void testHandleIndexerFailure_IrrecoverableBulkIndexException() {
        final BulkIndexingException e = new BulkIndexingException(
            randomAlphaOfLength(10),
            new ElasticsearchStatusException(randomAlphaOfLength(10), RestStatus.INTERNAL_SERVER_ERROR),
            true
        );
        assertRetryIfUnattendedOtherwiseFail(e);
    }

    public void testHandleIndexerFailure_RecoverableBulkIndexException() {
        final BulkIndexingException bulkIndexingException = new BulkIndexingException(
            randomAlphaOfLength(10),
            new ElasticsearchStatusException(randomAlphaOfLength(10), RestStatus.INTERNAL_SERVER_ERROR),
            false
        );

        List.of(true, false).forEach((unattended) -> { assertRetry(bulkIndexingException, unattended); });
    }

    public void testHandleIndexerFailure_ClusterBlockException() {
        List.of(true, false).forEach((unattended) -> {
            assertRetry(
                new ClusterBlockException(Map.of(randomAlphaOfLength(10), Set.of(MetadataIndexStateService.INDEX_CLOSED_BLOCK))),
                unattended
            );
        });
    }

    public void testHandleIndexerFailure_SearchPhaseExecutionExceptionWithNoShardSearchFailures() {
        List.of(true, false).forEach((unattended) -> {
            assertRetry(
                new SearchPhaseExecutionException(randomAlphaOfLength(10), randomAlphaOfLength(10), ShardSearchFailure.EMPTY_ARRAY),
                unattended
            );
        });
    }

    public void testHandleIndexerFailure_SearchPhaseExecutionExceptionWithShardSearchFailures() {
        List.of(true, false).forEach((unattended) -> {
            assertRetry(
                new SearchPhaseExecutionException(
                    randomAlphaOfLength(10),
                    randomAlphaOfLength(10),
                    new ShardSearchFailure[] { new ShardSearchFailure(new Exception()) }
                ),
                unattended
            );
        });
    }

    public void testHandleIndexerFailure_RecoverableElasticsearchException() {
        List.of(true, false).forEach((unattended) -> {
            assertRetry(new ElasticsearchStatusException(randomAlphaOfLength(10), RestStatus.INTERNAL_SERVER_ERROR), unattended);
        });
    }

    public void testHandleIndexerFailure_IrrecoverableElasticsearchException() {
        var e = new ElasticsearchStatusException(randomAlphaOfLength(10), RestStatus.NOT_FOUND);
        assertRetryIfUnattendedOtherwiseFail(e);
    }

    public void testHandleIndexerFailure_IllegalArgumentException() {
        var e = new IllegalArgumentException(randomAlphaOfLength(10));
        assertRetryIfUnattendedOtherwiseFail(e);
    }

    public void testHandleIndexerFailure_UnexpectedException() {
        List.of(true, false).forEach((unattended) -> { assertRetry(new Exception(), unattended); });
    }

    private void assertRetryIfUnattendedOtherwiseFail(Exception e) {
        List.of(true, false).forEach((unattended) -> {
            if (unattended) {
                assertRetry(e, unattended);
            } else {
                assertFailure(e);
            }
        });
    }

    private void assertRetry(Exception e, boolean unattended) {
        String transformId = randomAlphaOfLength(10);
        SettingsConfig settings = new SettingsConfig.Builder().setNumFailureRetries(2).setUnattended(unattended).build();

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        MockTransformContextListener contextListener = new MockTransformContextListener();
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);
        context.setPageSize(500);

        TransformFailureHandler handler = new TransformFailureHandler(auditor, context, transformId);

        assertNoFailure(handler, e, contextListener, settings, true);
        assertNoFailure(handler, e, contextListener, settings, true);
        if (unattended) {
            assertNoFailure(handler, e, contextListener, settings, true);
        } else {
            // fail after max retry attempts reached
            assertFailure(handler, e, contextListener, settings, true);
        }
    }

    private void assertClusterBlockHandled(Exception e, boolean unattended) {
        String transformId = randomAlphaOfLength(10);
        SettingsConfig settings = new SettingsConfig.Builder().setNumFailureRetries(2).setUnattended(unattended).build();

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        MockTransformContextListener contextListener = new MockTransformContextListener();
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);
        context.setPageSize(500);

        TransformFailureHandler handler = new TransformFailureHandler(auditor, context, transformId);

        assertNoFailure(handler, e, contextListener, settings, false);
        assertNoFailure(handler, e, contextListener, settings, false);
        assertNoFailure(handler, e, contextListener, settings, false);
        assertTrue(context.isWaitingForIndexToUnblock());
    }

    private void assertFailure(Exception e) {
        String transformId = randomAlphaOfLength(10);
        SettingsConfig settings = new SettingsConfig.Builder().setNumFailureRetries(2).build();

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        MockTransformContextListener contextListener = new MockTransformContextListener();
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);
        context.setPageSize(500);

        TransformFailureHandler handler = new TransformFailureHandler(auditor, context, transformId);

        assertFailure(handler, e, contextListener, settings, false);
    }

    private void assertNoFailure(
        TransformFailureHandler handler,
        Exception e,
        MockTransformContextListener mockTransformContextListener,
        SettingsConfig settings,
        boolean failureCountIncremented
    ) {
        handler.handleIndexerFailure(e, settings);
        assertFalse(mockTransformContextListener.getFailed());
        assertEquals(failureCountIncremented ? 1 : 0, mockTransformContextListener.getFailureCountChangedCounter());
        mockTransformContextListener.reset();
    }

    private void assertNoFailureAndContextPageSizeSet(Exception e, boolean unattended, int newPageSize) {
        String transformId = randomAlphaOfLength(10);
        SettingsConfig settings = new SettingsConfig.Builder().setNumFailureRetries(2).setUnattended(unattended).build();

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        MockTransformContextListener contextListener = new MockTransformContextListener();
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);
        context.setPageSize(500);

        TransformFailureHandler handler = new TransformFailureHandler(auditor, context, transformId);

        handler.handleIndexerFailure(e, settings);
        assertFalse(contextListener.getFailed());
        assertEquals(0, contextListener.getFailureCountChangedCounter());
        assertEquals(newPageSize, context.getPageSize());
        contextListener.reset();
    }

    private void assertFailure(
        TransformFailureHandler handler,
        Exception e,
        MockTransformContextListener mockTransformContextListener,
        SettingsConfig settings,
        boolean failureCountChanged
    ) {
        handler.handleIndexerFailure(e, settings);
        assertTrue(mockTransformContextListener.getFailed());
        assertEquals(failureCountChanged ? 1 : 0, mockTransformContextListener.getFailureCountChangedCounter());
        mockTransformContextListener.reset();
    }

}
