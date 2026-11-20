/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsqlUsageReportingPluginTests extends ESTestCase {

    private static Settings.Builder complete() {
        return Settings.builder()
            .put(UsageReportingSettings.ENABLED.getKey(), true)
            .put(UsageReportingSettings.URL.getKey(), "https://usage.example.com")
            .put(UsageReportingSettings.DEPLOYMENT_ID.getKey(), "d")
            .put(UsageReportingSettings.CLUSTER_ID.getKey(), "c")
            .put(UsageReportingSettings.PROVIDER.getKey(), "aws")
            .put(UsageReportingSettings.REGION.getKey(), "eu-west-1")
            .put(UsageReportingSettings.SSL_PREFIX + ".certificate", "client.crt")
            .put(UsageReportingSettings.SSL_PREFIX + ".key", "client.key");
    }

    /**
     * Runs {@code createComponents} with real settings, environment, thread pool and cluster service. Only the
     * {@link Plugin.PluginServices} interface is mocked: it has sixteen accessors of which the plugin reads four.
     */
    private void createComponents(EsqlUsageReportingPlugin plugin, Settings settings) throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
            NodeEnvironment nodeEnvironment = newNodeEnvironment()
        ) {
            Environment environment = TestEnvironment.newEnvironment(
                Settings.builder().put(settings).put("path.home", createTempDir()).build()
            );
            Plugin.PluginServices services = mock(Plugin.PluginServices.class);
            when(services.environment()).thenReturn(environment);
            when(services.clusterService()).thenReturn(clusterService);
            when(services.nodeEnvironment()).thenReturn(nodeEnvironment);
            when(services.threadPool()).thenReturn(threadPool);
            plugin.createComponents(services);
        } finally {
            terminate(threadPool);
        }
    }

    public void testDisabledByDefaultBuildsNothing() throws Exception {
        try (MockLog mockLog = MockLog.capture(EsqlUsageReportingPlugin.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no error", EsqlUsageReportingPlugin.class.getCanonicalName(), Level.ERROR, "*")
            );
            EsqlUsageReportingPlugin plugin = new EsqlUsageReportingPlugin();
            createComponents(plugin, Settings.EMPTY);
            assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
            assertThat(plugin.reportExempt(), is(false));
            mockLog.assertAllExpectationsMatched();
            plugin.close();
        }
    }

    public void testEnabledAndCompleteBuildsComponents() throws Exception {
        try (MockLog mockLog = MockLog.capture(EsqlUsageReportingPlugin.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no error", EsqlUsageReportingPlugin.class.getCanonicalName(), Level.ERROR, "*")
            );
            EsqlUsageReportingPlugin plugin = new EsqlUsageReportingPlugin();
            createComponents(plugin, complete().put(UsageReportingSettings.REPORT_EXEMPT.getKey(), true).build());
            assertThat(plugin.reportExempt(), is(true));
            mockLog.assertAllExpectationsMatched();
            plugin.close();
        }
    }

    public void testFailureToBuildLogsOneErrorAndStaysInactive() throws Exception {
        try (MockLog mockLog = MockLog.capture(EsqlUsageReportingPlugin.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "failed to start",
                    EsqlUsageReportingPlugin.class.getCanonicalName(),
                    Level.ERROR,
                    "usage reporting failed to start: it stays inactive and nothing is reported"
                )
            );
            EsqlUsageReportingPlugin plugin = new EsqlUsageReportingPlugin() {
                @Override
                void activate(UsageReportingSettings settings, PluginServices services) {
                    throw new IllegalStateException("boom");
                }
            };
            createComponents(plugin, complete().put(UsageReportingSettings.REPORT_EXEMPT.getKey(), true).build());
            assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
            assertThat(plugin.reportExempt(), is(false));
            mockLog.assertAllExpectationsMatched();
            plugin.close();
        }
    }

    /**
     * A plugin whose reporter and publisher only record when they are closed, because the real ones do not exist yet and the order
     * matters: the final flush of the reporter needs the publisher.
     */
    private static class RecordingPlugin extends EsqlUsageReportingPlugin {
        final List<String> events = new ArrayList<>();
        private final boolean failToBuildReporter;
        private final boolean failToStartReporter;
        boolean failOnClose;
        boolean reporterCloseError;

        RecordingPlugin(boolean failToBuildReporter, boolean failToStartReporter) {
            this.failToBuildReporter = failToBuildReporter;
            this.failToStartReporter = failToStartReporter;
        }

        @Override
        UsagePublisher buildPublisher(UsageReportingSettings settings) {
            return new UsagePublisher() {
                @Override
                public PublishResult publish(List<UsageRecord> records) {
                    return PublishResult.empty();
                }

                @Override
                public void close() {
                    events.add("publisher closed");
                    if (failOnClose) {
                        throw new IllegalStateException("publisher close failed");
                    }
                }
            };
        }

        @Override
        UsageReporter buildReporter(
            UsageReportingSettings settings,
            UsageAccumulator accumulator,
            UsagePublisher publisher,
            SourceIdentity identity,
            PluginServices services
        ) {
            if (failToBuildReporter) {
                throw new IllegalStateException("cannot build");
            }
            return new UsageReporter() {
                @Override
                public void start() {
                    if (failToStartReporter) {
                        throw new IllegalStateException("cannot start");
                    }
                }

                @Override
                public void tick(Instant now) {}

                @Override
                public void close() {
                    events.add("reporter closed");
                    if (reporterCloseError) {
                        throw new AssertionError("reporter close error");
                    }
                    if (failOnClose) {
                        throw new IllegalStateException("reporter close failed");
                    }
                }
            };
        }
    }

    public void testFailureToStartTheReporterClosesItBeforeThePublisherAndStaysInactive() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(false, true);
        createComponents(plugin, complete().put(UsageReportingSettings.REPORT_EXEMPT.getKey(), true).build());
        assertThat(plugin.events, equalTo(List.of("reporter closed", "publisher closed")));
        assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
        assertThat(plugin.reportExempt(), is(false));
        plugin.close();
        assertThat(plugin.events, equalTo(List.of("reporter closed", "publisher closed")));
    }

    public void testFailureToBuildTheReporterClosesThePublisher() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(true, false);
        createComponents(plugin, complete().build());
        assertThat(plugin.events, equalTo(List.of("publisher closed")));
        assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
    }

    public void testCloseClosesTheReporterBeforeThePublisher() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(false, false);
        createComponents(plugin, complete().build());
        assertThat(plugin.events, equalTo(List.of()));
        plugin.close();
        assertThat(plugin.events, equalTo(List.of("reporter closed", "publisher closed")));
    }

    /** The failure of the final flush must stay visible when closing the publisher fails too. */
    public void testCloseKeepsTheReporterFailureAndAttachesThePublisherFailure() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(false, false);
        createComponents(plugin, complete().build());
        plugin.failOnClose = true;
        Exception e = expectThrows(Exception.class, plugin::close);
        assertThat(e.getMessage(), equalTo("reporter close failed"));
        assertThat(e.getSuppressed().length, equalTo(1));
        assertThat(e.getSuppressed()[0].getMessage(), equalTo("publisher close failed"));
        assertThat(plugin.events, equalTo(List.of("reporter closed", "publisher closed")));
    }

    /** Not only exceptions: the publisher must be closed even if the final flush of the reporter throws an Error. */
    public void testPublisherIsClosedEvenIfTheReporterThrowsAnError() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(false, false);
        createComponents(plugin, complete().build());
        plugin.reporterCloseError = true;
        expectThrows(AssertionError.class, plugin::close);
        assertThat(plugin.events, equalTo(List.of("reporter closed", "publisher closed")));
    }

    public void testEnabledButIncompleteLogsOneErrorAndStaysInactive() throws Exception {
        try (MockLog mockLog = MockLog.capture(EsqlUsageReportingPlugin.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "missing settings",
                    EsqlUsageReportingPlugin.class.getCanonicalName(),
                    Level.ERROR,
                    "usage reporting is enabled but its configuration is incomplete or invalid: "
                        + "[xpack.esql.usage_reporting.url, xpack.esql.usage_reporting.region]; it stays inactive"
                )
            );
            EsqlUsageReportingPlugin plugin = new EsqlUsageReportingPlugin();
            Settings incomplete = complete().putNull(UsageReportingSettings.URL.getKey())
                .putNull(UsageReportingSettings.REGION.getKey())
                .put(UsageReportingSettings.REPORT_EXEMPT.getKey(), true)
                .build();
            createComponents(plugin, incomplete);
            assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
            assertThat(plugin.reportExempt(), is(false));
            mockLog.assertAllExpectationsMatched();
            plugin.close();
        }
    }
}
