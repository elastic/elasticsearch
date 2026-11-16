/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.plugins.Plugin;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

/**
 * Checks that a pause script that gives up waiting fails the query instead of leaving it running.
 * Pausing tests rely on this so that a stuck pause surfaces as a query failure rather than a hang
 * that leaks search contexts and breaker bytes into later tests.
 */
public class PauseFieldPluginFailureIT extends AbstractPausableIntegTestCase {

    private static volatile boolean throwFromWait;

    @Override
    protected Class<? extends Plugin> pausableFieldPluginClass() {
        return GivingUpPauseFieldPlugin.class;
    }

    public void testQueryFailsWhenWaitTimesOut() {
        throwFromWait = false;
        assertThat(queryFailure(), containsString("timed out waiting in [pause] script"));
    }

    public void testQueryFailsWhenWaitThrowsAssertionError() {
        throwFromWait = true;
        assertThat(
            queryFailure(),
            allOf(containsString("assertion failed waiting in [pause] script"), containsString("assertion in onWait"))
        );
    }

    private String queryFailure() {
        EsqlQueryRequest request = syncEsqlQueryRequest();
        request.query("FROM test | STATS sum(pause_me)");
        request.pragmas(randomPragmas());
        request.allowPartialResults(false);
        Exception e = expectThrows(Exception.class, () -> run(request).close());
        return ExceptionsHelper.stackTrace(e);
    }

    public static class GivingUpPauseFieldPlugin extends AbstractPauseFieldPlugin {
        @Override
        protected boolean onWait() {
            if (throwFromWait) {
                throw new AssertionError("assertion in onWait");
            }
            return false;
        }
    }
}
