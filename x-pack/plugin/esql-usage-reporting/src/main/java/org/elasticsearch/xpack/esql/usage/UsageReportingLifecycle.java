/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;

/**
 * Ties the {@link UsageReporter} and the {@link UsagePublisher} to the life cycle of the node, because they run threads and hold
 * connections: the node starts the reporter after it has been built, stops it (its final flush) while the node is still running,
 * and closes the publisher last, since the final flush needs it. This keeps the reporter's own interface, {@code start()} and
 * {@code close()}, independent of the node.
 * <p>
 * A reporter that fails to start must not stop the node: the failure is logged once and reported through {@code onStartFailure},
 * which makes the plugin inactive again.
 */
final class UsageReportingLifecycle extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(UsageReportingLifecycle.class);

    private final UsageReporter reporter;
    private final UsagePublisher publisher;
    private final Runnable onStartFailure;
    private boolean reporterClosed;

    UsageReportingLifecycle(UsageReporter reporter, UsagePublisher publisher, Runnable onStartFailure) {
        this.reporter = reporter;
        this.publisher = publisher;
        this.onStartFailure = onStartFailure;
    }

    @Override
    protected void doStart() {
        try {
            reporter.start();
        } catch (Exception e) {
            logger.error("usage reporting failed to start: it stays inactive and nothing is reported", e);
            onStartFailure.run();
        }
    }

    @Override
    protected void doStop() {
        closeReporter();
    }

    @Override
    protected void doClose() throws IOException {
        // Closing without a stop (the node never started) still has to close the reporter, which is idempotent here.
        try {
            closeReporter();
        } finally {
            publisher.close();
        }
    }

    private void closeReporter() {
        if (reporterClosed == false) {
            reporterClosed = true;
            reporter.close();
        }
    }
}
