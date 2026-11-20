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
    private boolean publisherClosed;

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
            // Release at once: the node keeps serving, and nothing else would stop a partly started reporter before the shutdown.
            try {
                closeReporter();
            } catch (Exception closeFailure) {
                logger.warn("failed to close the usage reporter after it failed to start", closeFailure);
            }
            try {
                closePublisher();
            } catch (Exception closeFailure) {
                logger.warn("failed to close the usage publisher after the reporter failed to start", closeFailure);
            }
            onStartFailure.run();
        }
    }

    @Override
    protected void doStop() {
        // A failed final flush loses usage, which can only under-count, but must not abort the shutdown of the node: Node#close calls
        // stop and does not guard it, so an exception here would skip the rest of the node's close list.
        try {
            closeReporter();
        } catch (Exception e) {
            logger.warn("failed to flush the usage of this node while stopping: some usage may not be reported", e);
        }
    }

    @Override
    protected void doClose() throws IOException {
        // Closing without a stop (the node never started) still has to close the reporter; the closes are idempotent here.
        try {
            closeReporter();
        } finally {
            closePublisher();
        }
    }

    private void closeReporter() {
        if (reporterClosed == false) {
            reporterClosed = true;
            reporter.close();
        }
    }

    private void closePublisher() {
        if (publisherClosed == false) {
            publisherClosed = true;
            publisher.close();
        }
    }
}
