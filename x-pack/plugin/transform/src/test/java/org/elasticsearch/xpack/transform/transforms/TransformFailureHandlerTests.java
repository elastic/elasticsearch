/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;

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
        public void fail(String failureMessage, ActionListener<Void> listener) {
            failed = true;
        }

        public boolean getFailed() {
            return failed;
        }

        public int getFailureCountChangedCounter() {
            return failureCountChangedCounter;
        }
    }

    public void testUnattended() {
        String transformId = randomAlphaOfLength(10);
        SettingsConfig settings = new SettingsConfig.Builder().setUnattended(true).build();

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        MockTransformContextListener contextListener = new MockTransformContextListener();
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);
        context.setPageSize(500);

        TransformFailureHandler handler = new TransformFailureHandler(auditor, context, transformId);

        handler.handleIndexerFailure(
            new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] {
                    new ShardSearchFailure(new CircuitBreakingException("to much memory", 110, 100, CircuitBreaker.Durability.TRANSIENT)) }
            ),
            settings
        );

        // CBE isn't a failure, but it only affects page size(which we don't test here)
        assertFalse(contextListener.getFailed());
        assertEquals(0, contextListener.getFailureCountChangedCounter());

        assertNoFailure(
            handler,
            new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] {
                    new ShardSearchFailure(
                        new ScriptException(
                            "runtime error",
                            new ArithmeticException("/ by zero"),
                            singletonList("stack"),
                            "test",
                            "painless"
                        )
                    ) }
            ),
            contextListener,
            settings
        );
        assertNoFailure(
            handler,
            new ElasticsearchStatusException("something really bad happened", RestStatus.INTERNAL_SERVER_ERROR),
            contextListener,
            settings
        );
        assertNoFailure(handler, new IllegalArgumentException("expected apples not oranges"), contextListener, settings);
        assertNoFailure(handler, new RuntimeException("the s*** hit the fan"), contextListener, settings);
        assertNoFailure(handler, new NullPointerException("NPE"), contextListener, settings);
    }

    private void assertNoFailure(
        TransformFailureHandler handler,
        Exception e,
        MockTransformContextListener mockTransformContextListener,
        SettingsConfig settings
    ) {
        handler.handleIndexerFailure(e, settings);
        assertFalse(mockTransformContextListener.getFailed());
        assertEquals(1, mockTransformContextListener.getFailureCountChangedCounter());
        mockTransformContextListener.reset();
    }

}
