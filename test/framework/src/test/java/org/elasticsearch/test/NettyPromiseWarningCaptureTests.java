/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.hamcrest.Matchers.containsString;

/**
 * A promise listener that throws is normally one of our own callbacks breaking, so {@link ESTestCase} captures Netty's
 * report of it and fails the test. A few third-party libraries throw from their own listeners during their teardown, and
 * those messages are tolerated.
 * <p>
 * Both halves are pinned here, and the second is what makes the first mean anything: if the capture were silently dead —
 * a log level raised above WARN, an appender detached, a match that swallows everything — the first test would pass while
 * nothing at all was being checked. They only discriminate together, so running either alone, as a {@code tests.method}
 * reproduce line does, proves nothing.
 */
public class NettyPromiseWarningCaptureTests extends ESTestCase {

    private static final Logger PROMISE_LOGGER = LogManager.getLogger("io.netty.util.concurrent.DefaultPromise");

    /**
     * Verbatim from the CI failure in #161727. The listener is a lambda, so what Netty reports is the class that declares
     * it followed by a generated suffix carrying an address that differs from run to run. The declaring class is the
     * stable part, and is what the match is keyed on.
     */
    private static final String AWS_SDK_POOL_SHUTDOWN = "An exception was thrown by software.amazon.awssdk.http.nio.netty"
        + ".internal.IdleConnectionCountingChannelPool$$Lambda/0x0000000031059400.operationComplete()";

    public void testAThirdPartyShutdownWarningDoesNotFailTheTest() throws Exception {
        PROMISE_LOGGER.warn(AWS_SDK_POOL_SHUTDOWN);
        checkStaticState();
    }

    public void testTheRestOfThatLibraryIsStillWatched() {
        // Same package as the tolerated pool, but this listener sits on the request path: it dispatches onError to the
        // response handler from inside operationComplete, so a throw reaching it is worth a failure.
        PROMISE_LOGGER.warn(
            "An exception was thrown by software.amazon.awssdk.http.nio.netty.internal"
                + ".NettyRequestExecutor$$Lambda/0x0000000031059400.operationComplete()"
        );
        AssertionError thrown = expectThrows(AssertionError.class, ESTestCase::checkStaticState);
        assertThat(thrown.getMessage(), containsString("NettyRequestExecutor"));
    }

    public void testEveryOtherPromiseWarningStillFailsTheTest() {
        PROMISE_LOGGER.warn("An exception was thrown by org.elasticsearch.ImaginaryListener.operationComplete()");
        // checkStaticState clears what it captured, so this leaves nothing behind for the trailing @After to trip over.
        AssertionError thrown = expectThrows(AssertionError.class, ESTestCase::checkStaticState);
        assertThat(thrown.getMessage(), containsString("ImaginaryListener"));
    }
}
