/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.time.Clock;
import java.util.Collection;
import java.util.List;

/**
 * Reports the CPU time of ES|QL queries over external sources to the Elastic Usage API, so that Data Federation can be metered on
 * hosted deployments. It is a separate plugin so that it ships in the default distribution but does nothing unless Cloud enables it
 * with {@code xpack.esql.usage_reporting.enabled}.
 * <p>
 * It is loaded only where stateless is not enabled ({@code STATEFUL_ONLY}): Serverless has its own collector, and two meters would
 * count the same queries twice, the worst error possible here.
 * <p>
 * Life cycle: the {@link UsageListener} is created by ES|QL's extension loader before anything else and asks this plugin for the
 * {@link #accumulator()}, a no-op until {@link #createComponents} has built the real pieces. That happens only when the plugin
 * is enabled and its settings are complete; if they are not, one ERROR lists what is wrong and the plugin stays inactive.
 * The reporter and the publisher are returned from {@code createComponents} as one {@link UsageReportingLifecycle}, so that the
 * node, not this plugin, starts the reporter once the node starts, stops it (final flush) while the node is still running, and
 * closes the publisher.
 */
public class EsqlUsageReportingPlugin extends Plugin {

    private static final Logger logger = LogManager.getLogger(EsqlUsageReportingPlugin.class);

    private volatile UsageAccumulator accumulator = NoopUsageAccumulator.INSTANCE;
    private volatile boolean reportExempt;

    @Override
    public List<Setting<?>> getSettings() {
        return UsageReportingSettings.settings();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        UsageReportingSettings settings = UsageReportingSettings.of(services.environment().settings());
        if (settings.enabled() == false) {
            return List.of();
        }
        List<String> problems = settings.problems();
        if (problems.isEmpty() == false) {
            logger.error("usage reporting is enabled but its configuration is incomplete or invalid: {}; it stays inactive", problems);
            return List.of();
        }
        // Like missing settings, a failure to build the pieces must not stop the node: it stays inactive.
        try {
            return List.of(activate(settings, services));
        } catch (Exception e) {
            logger.error("usage reporting failed to start: it stays inactive and nothing is reported", e);
            return List.of();
        }
    }

    /**
     * Builds the pieces and, only when all of them are built, makes the accumulator visible to the listener, so that a failure
     * leaves the plugin inactive. Nothing runs yet: the reporter starts with the node.
     */
    UsageReportingLifecycle activate(UsageReportingSettings settings, PluginServices services) {
        // The node id comes from the node environment: the cluster service has no local node until the node has started,
        // which is after createComponents.
        SourceIdentity identity = new SourceIdentity(
            services.nodeEnvironment().nodeId(),
            settings.deploymentId(),
            settings.clusterId(),
            settings.provider(),
            settings.region()
        );
        UsageAccumulator newAccumulator = UsageComponents.newAccumulator(settings);
        UsagePublisher newPublisher = buildPublisher(settings);
        UsageReporter newReporter;
        try {
            newReporter = buildReporter(settings, newAccumulator, newPublisher, identity, services);
        } catch (RuntimeException e) {
            Releasables.closeWhileHandlingException(newPublisher);
            throw e;
        }
        this.reportExempt = settings.reportExempt();
        this.accumulator = newAccumulator;
        return new UsageReportingLifecycle(newReporter, newPublisher, this::deactivate);
    }

    /** The reporter could not start: the listener goes back to a no-op accumulator, and the node keeps running. */
    private void deactivate() {
        this.accumulator = NoopUsageAccumulator.INSTANCE;
        this.reportExempt = false;
    }

    /** Builds the publisher; a seam so that tests can observe when it is closed. */
    UsagePublisher buildPublisher(UsageReportingSettings settings) {
        return UsageComponents.newPublisher(settings, sslProfile());
    }

    /**
     * The profile of {@code xpack.esql.usage_reporting.ssl}. X-pack-core builds its SSL service, which resolves the prefix declared
     * by {@link UsageSslProfile}, before this plugin's {@code createComponents}. A seam, because there is no SSL service in a unit
     * test.
     */
    SslProfile sslProfile() {
        return XPackPlugin.getSharedSslService().profile(UsageReportingSettings.SSL_PREFIX);
    }

    /** Builds the reporter with the other pieces; a seam so that tests can make the reporter fail. */
    UsageReporter buildReporter(
        UsageReportingSettings settings,
        UsageAccumulator accumulator,
        UsagePublisher publisher,
        SourceIdentity identity,
        PluginServices services
    ) {
        return UsageComponents.newReporter(
            settings,
            accumulator,
            UsageComponents.newRecordBuilder(settings),
            UsageComponents.newPendingStore(settings),
            publisher,
            identity,
            services.threadPool(),
            Clock.systemUTC()
        );
    }

    /** The accumulator that {@link UsageListener} feeds: a no-op until the plugin has built the real one. */
    UsageAccumulator accumulator() {
        return accumulator;
    }

    /** Whether the usage of queries that did not succeed is to be accumulated, as exempt. */
    boolean reportExempt() {
        return reportExempt;
    }
}
