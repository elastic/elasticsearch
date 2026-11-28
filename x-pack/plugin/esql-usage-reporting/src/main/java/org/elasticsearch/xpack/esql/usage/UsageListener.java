/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The entry point from ES|QL: called once per completed query that read external sources, on the coordinating node, from the
 * query thread. It must be cheap and non-blocking (no I/O, no locks), and it never throws, so that usage reporting can never fail a query.
 * <p>
 * Queries that did not succeed are ignored unless {@code report_exempt} is set, in which case they are accumulated apart and
 * reported as exempt. The check is here and not in the accumulator, which stays generic.
 * <p>
 * The extension loader creates this class before the plugin has its settings, so it takes the plugin and asks it for the
 * accumulator on each call: a no-op one until the real one is built.
 */
public final class UsageListener implements QueryMetricsListener {

    private static final Logger logger = LogManager.getLogger(UsageListener.class);

    private final EsqlUsageReportingPlugin plugin;
    private final AtomicBoolean failureLogged = new AtomicBoolean();

    public UsageListener(EsqlUsageReportingPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public void onQueryCompleted(Map<String, Long> metrics) {
        onQueryCompleted(Outcome.SUCCESS, metrics);
    }

    @Override
    public void onQueryCompleted(Outcome outcome, Map<String, Long> metrics) {
        try {
            if (outcome != Outcome.SUCCESS && plugin.reportExempt() == false) {
                return;
            }
            plugin.accumulator().record(outcome, metrics);
        } catch (RuntimeException e) {
            // Logged once: a persistent failure would otherwise flood the log with one line per query.
            if (failureLogged.compareAndSet(false, true)) {
                logger.warn("failed to record the usage of a query; further failures are not logged", e);
            }
        }
    }
}
