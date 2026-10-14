/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * A promise listener that throws is normally one of our own callbacks breaking, so {@link ESTestCase} captures Netty's
 * report of it and fails teardown. A couple of third-party libraries throw from their own listeners while shutting down,
 * and those messages are tolerated by name.
 * <p>
 * Each tolerated entry is pinned here, and so is the other direction: a message from elsewhere in the same library, and
 * one of our own, must still fail. Those are what give the tolerated cases meaning — if the capture were dead, or a
 * match too wide, the tolerated cases would pass while nothing was being checked at all. They only discriminate
 * together, so running any one of them alone proves nothing.
 */
public class ESTestCasePromiseWarningTests extends ESTestCase {

    /**
     * Verbatim from the CI failure in #161727. The listener is a lambda, so what Netty reports is the class that declares
     * it followed by a generated suffix carrying an address that differs from run to run. The declaring class is the
     * stable part, and is what the match is keyed on.
     */
    private static final String AWS_SDK_POOL_SHUTDOWN = "An exception was thrown by software.amazon.awssdk.http.nio.netty"
        + ".internal.IdleConnectionCountingChannelPool$$Lambda/0x0000000031059400.operationComplete()";

    /** The shape reported for the Arrow Flight tests in #144244 and #144263. */
    private static final String GRPC_STREAM_SHUTDOWN = "An exception was thrown by io.grpc.netty.NettyServerHandler$5.operationComplete()";

    /**
     * Same library and same package as the tolerated pool, but this listener is on the request path: it dispatches
     * onError to the response handler from inside operationComplete, so a throw reaching it is worth a failure.
     */
    private static final String AWS_SDK_REQUEST_PATH = "An exception was thrown by software.amazon.awssdk.http.nio.netty"
        + ".internal.NettyRequestExecutor$$Lambda/0x0000000031059400.operationComplete()";

    public void testTheAwsSdkPoolShutdownWarningPassesTeardown() throws Exception {
        new PromiseWarningFixture(AWS_SDK_POOL_SHUTDOWN).assertTeardownPasses();
    }

    public void testTheGrpcStreamShutdownWarningPassesTeardown() throws Exception {
        new PromiseWarningFixture(GRPC_STREAM_SHUTDOWN).assertTeardownPasses();
    }

    public void testTheRestOfThatLibraryStillFailsTeardown() throws Exception {
        new PromiseWarningFixture(AWS_SDK_REQUEST_PATH).assertTeardownFails("NettyRequestExecutor");
    }

    public void testOneOfOurOwnStillFailsTeardown() throws Exception {
        String ours = "An exception was thrown by org.elasticsearch.ImaginaryListener.operationComplete()";
        new PromiseWarningFixture(ours).assertTeardownFails("ImaginaryListener");
    }

    /**
     * Raises one warning on the logger {@link ESTestCase} watches and then runs a real teardown over it. The capture is
     * static, and teardown clears it either way, so nothing survives into the enclosing test.
     */
    private static final class PromiseWarningFixture extends ESTestCase {

        private static final Logger PROMISE_LOGGER = LogManager.getLogger("io.netty.util.concurrent.DefaultPromise");

        private final String message;

        PromiseWarningFixture(String message) {
            this.message = message;
        }

        void assertTeardownPasses() throws Exception {
            before();
            PROMISE_LOGGER.warn(message);
            after();
        }

        void assertTeardownFails(String expectedInMessage) throws Exception {
            before();
            PROMISE_LOGGER.warn(message);
            AssertionError thrown = expectThrows(AssertionError.class, this::after);
            assertThat(thrown.getMessage(), containsString(expectedInMessage));
        }
    }
}
