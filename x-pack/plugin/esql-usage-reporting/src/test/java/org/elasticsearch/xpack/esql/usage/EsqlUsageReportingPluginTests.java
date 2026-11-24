/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleComponent;
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
import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
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

    /** A unit test has no SSL service of x-pack-core to resolve the profile from, and the no-op publisher does not need one. */
    private static class TestPlugin extends EsqlUsageReportingPlugin {
        @Override
        SslProfile sslProfile() {
            return null;
        }
    }

    /**
     * Runs {@code createComponents} with real settings, environment, thread pool and cluster service. Only the
     * {@link Plugin.PluginServices} interface is mocked: it has sixteen accessors of which the plugin reads four.
     */
    private Collection<?> createComponents(EsqlUsageReportingPlugin plugin, Settings settings) throws Exception {
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
            return plugin.createComponents(services);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * A plugin whose reporter and publisher only record what is done to them, because the real ones do not exist yet and the order
     * matters: the final flush of the reporter needs the publisher.
     */
    private static class RecordingPlugin extends TestPlugin {
        final List<String> events = new ArrayList<>();
        private final boolean failToBuildReporter;
        private final boolean failToStartReporter;
        boolean reporterCloseError;
        boolean reporterCloseFailure;
        boolean buildError;

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
            if (buildError) {
                throw new AssertionError("build error");
            }
            if (failToBuildReporter) {
                throw new IllegalStateException("cannot build");
            }
            return new UsageReporter() {
                @Override
                public void start() {
                    events.add("reporter started");
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
                    if (reporterCloseFailure) {
                        throw new IllegalStateException("flush failed");
                    }
                }
            };
        }
    }

    private static LifecycleComponent lifecycleOf(Collection<?> components) {
        assertThat(components.size(), equalTo(1));
        Object component = components.iterator().next();
        assertThat(component, instanceOf(LifecycleComponent.class));
        return (LifecycleComponent) component;
    }

    public void testDisabledByDefaultBuildsNothing() throws Exception {
        try (MockLog mockLog = MockLog.capture(EsqlUsageReportingPlugin.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no error", EsqlUsageReportingPlugin.class.getCanonicalName(), Level.ERROR, "*")
            );
            EsqlUsageReportingPlugin plugin = new TestPlugin();
            assertThat(createComponents(plugin, Settings.EMPTY), empty());
            assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
            assertThat(plugin.reportExempt(), is(false));
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testEnabledAndCompleteReturnsALifecycleComponent() throws Exception {
        try (MockLog mockLog = MockLog.capture(EsqlUsageReportingPlugin.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no error", EsqlUsageReportingPlugin.class.getCanonicalName(), Level.ERROR, "*")
            );
            EsqlUsageReportingPlugin plugin = new TestPlugin();
            LifecycleComponent component = lifecycleOf(
                createComponents(plugin, complete().put(UsageReportingSettings.REPORT_EXEMPT.getKey(), true).build())
            );
            assertThat(plugin.reportExempt(), is(true));
            assertThat(plugin.accumulator(), instanceOf(AtomicUsageAccumulator.class));
            component.start();
            component.close();
            mockLog.assertAllExpectationsMatched();
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
            EsqlUsageReportingPlugin plugin = new TestPlugin() {
                @Override
                UsageReportingLifecycle activate(UsageReportingSettings settings, PluginServices services) {
                    throw new IllegalStateException("boom");
                }
            };
            assertThat(createComponents(plugin, complete().put(UsageReportingSettings.REPORT_EXEMPT.getKey(), true).build()), empty());
            assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
            assertThat(plugin.reportExempt(), is(false));
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testFailureToBuildTheReporterClosesThePublisher() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(true, false);
        assertThat(createComponents(plugin, complete().build()), empty());
        assertThat(plugin.events, equalTo(List.of("publisher closed")));
        assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
    }

    /** Nothing runs while the components are built: the node starts the reporter, stops it, and then closes the publisher. */
    public void testTheNodeStartsAndStopsTheReporterAndClosesThePublisherLast() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(false, false);
        LifecycleComponent component = lifecycleOf(createComponents(plugin, complete().build()));
        assertThat(plugin.events, empty());
        component.start();
        assertThat(plugin.events, equalTo(List.of("reporter started")));
        component.stop();
        assertThat(plugin.events, equalTo(List.of("reporter started", "reporter closed")));
        component.close();
        assertThat(plugin.events, equalTo(List.of("reporter started", "reporter closed", "publisher closed")));
    }

    public void testCloseWithoutStartClosesTheReporterThenThePublisher() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(false, false);
        lifecycleOf(createComponents(plugin, complete().build())).close();
        assertThat(plugin.events, equalTo(List.of("reporter closed", "publisher closed")));
    }

    /** A reporter that cannot start must not stop the node: one ERROR, and the plugin is inactive again. */
    public void testFailureToStartTheReporterIsLoggedAndTheNodeKeepsRunning() throws Exception {
        try (MockLog mockLog = MockLog.capture(UsageReportingLifecycle.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "failed to start",
                    UsageReportingLifecycle.class.getCanonicalName(),
                    Level.ERROR,
                    "usage reporting failed to start: it stays inactive and nothing is reported"
                )
            );
            RecordingPlugin plugin = new RecordingPlugin(false, true);
            LifecycleComponent component = lifecycleOf(
                createComponents(plugin, complete().put(UsageReportingSettings.REPORT_EXEMPT.getKey(), true).build())
            );
            assertThat(plugin.reportExempt(), is(true));
            component.start();
            assertThat(plugin.reportExempt(), is(false));
            assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
            mockLog.assertAllExpectationsMatched();
            // Released at once, not at the shutdown of the node, and the later stop and close do not release again.
            assertThat(plugin.events, equalTo(List.of("reporter started", "reporter closed", "publisher closed")));
            component.stop();
            component.close();
            assertThat(plugin.events, equalTo(List.of("reporter started", "reporter closed", "publisher closed")));
        }
    }

    /** Every step of the release is attempted, an Error from the reporter's close included, which still propagates. */
    public void testAnErrorClosingTheReporterAfterAFailedStartDoesNotSkipTheRest() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(false, true);
        LifecycleComponent component = lifecycleOf(
            createComponents(plugin, complete().put(UsageReportingSettings.REPORT_EXEMPT.getKey(), true).build())
        );
        plugin.reporterCloseError = true;
        expectThrows(AssertionError.class, component::start);
        assertThat(plugin.events, equalTo(List.of("reporter started", "reporter closed", "publisher closed")));
        assertThat(plugin.reportExempt(), is(false));
        assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
    }

    /** Whatever is thrown, an Error included, nothing owns the publisher yet, so it is closed. */
    public void testAnErrorWhileBuildingTheReporterClosesThePublisher() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(false, false);
        plugin.buildError = true;
        expectThrows(AssertionError.class, () -> createComponents(plugin, complete().build()));
        assertThat(plugin.events, equalTo(List.of("publisher closed")));
    }

    /** Node#close calls stop without a guard: a failed final flush must not abort the shutdown of the node. */
    public void testAFailedFinalFlushDoesNotAbortTheStop() throws Exception {
        try (MockLog mockLog = MockLog.capture(UsageReportingLifecycle.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "flush failed",
                    UsageReportingLifecycle.class.getCanonicalName(),
                    Level.WARN,
                    "failed to flush the usage of this node while stopping: some usage may not be reported"
                )
            );
            RecordingPlugin plugin = new RecordingPlugin(false, false);
            LifecycleComponent component = lifecycleOf(createComponents(plugin, complete().build()));
            component.start();
            plugin.reporterCloseFailure = true;
            component.stop();
            mockLog.assertAllExpectationsMatched();
            component.close();
            assertThat(plugin.events, equalTo(List.of("reporter started", "reporter closed", "publisher closed")));
        }
    }

    /** Not only exceptions: the publisher must be closed even if the final flush of the reporter throws an Error. */
    public void testPublisherIsClosedEvenIfStoppingTheReporterThrowsAnError() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(false, false);
        LifecycleComponent component = lifecycleOf(createComponents(plugin, complete().build()));
        component.start();
        plugin.reporterCloseError = true;
        expectThrows(AssertionError.class, component::stop);
        component.close();
        assertThat(plugin.events, equalTo(List.of("reporter started", "reporter closed", "publisher closed")));
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
            EsqlUsageReportingPlugin plugin = new TestPlugin();
            Settings incomplete = complete().putNull(UsageReportingSettings.URL.getKey())
                .putNull(UsageReportingSettings.REGION.getKey())
                .put(UsageReportingSettings.REPORT_EXEMPT.getKey(), true)
                .build();
            assertThat(createComponents(plugin, incomplete), empty());
            assertThat(plugin.accumulator(), sameInstance(NoopUsageAccumulator.INSTANCE));
            assertThat(plugin.reportExempt(), is(false));
            mockLog.assertAllExpectationsMatched();
        }
    }
}
