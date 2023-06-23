/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.exporter.ExportException;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MonitoringServiceTests extends ESTestCase {

    private TestThreadPool threadPool;
    private MonitoringService monitoringService;
    private XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private SSLService sslService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();

        final Monitoring monitoring = new Monitoring(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }
        };
        clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(monitoring.getSettings()));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        sslService = mock(SSLService.class);
    }

    @After
    public void terminate() throws Exception {
        if (monitoringService != null) {
            monitoringService.close();
        }
        terminate(threadPool);
    }

    public void testIsMonitoringActive() throws Exception {
        monitoringService = new MonitoringService(Settings.EMPTY, clusterService, threadPool, emptySet(), new CountingExporter());

        monitoringService.start();
        assertBusy(() -> assertTrue(monitoringService.isStarted()));
        assertFalse(monitoringService.isMonitoringActive());

        monitoringService.setMonitoringActive(true);
        assertTrue(monitoringService.isMonitoringActive());

        monitoringService.stop();
        assertBusy(() -> assertFalse(monitoringService.isStarted()));
        assertFalse(monitoringService.isMonitoringActive());

        monitoringService.start();
        assertBusy(() -> assertTrue(monitoringService.isStarted()));
        assertTrue(monitoringService.isMonitoringActive());

        monitoringService.close();
        assertBusy(() -> assertFalse(monitoringService.isStarted()));
        assertFalse(monitoringService.isMonitoringActive());
    }

    public void testInterval() throws Exception {
        final Settings settings = Settings.builder().put("xpack.monitoring.collection.interval", MonitoringService.MIN_INTERVAL).build();

        CountingExporter exporter = new CountingExporter();
        monitoringService = new MonitoringService(settings, clusterService, threadPool, emptySet(), exporter);

        monitoringService.start();
        assertBusy(() -> assertTrue(monitoringService.isStarted()));
        assertFalse("interval does not start the monitoring execution", monitoringService.isMonitoringActive());
        assertEquals(0, exporter.getExportsCount());

        monitoringService.setMonitoringActive(true);
        assertTrue(monitoringService.isMonitoringActive());

        // now the interval should take place
        assertBusy(() -> assertThat(exporter.getExportsCount(), greaterThan(0)));

        // take down threads
        monitoringService.setMonitoringActive(false);
        assertWarnings(
            "[xpack.monitoring.collection.interval] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testSkipExecution() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final BlockingExporter exporter = new BlockingExporter(latch);
        final Settings settings = Settings.builder()
            .put("xpack.monitoring.collection.enabled", true)
            .put("xpack.monitoring.collection.interval", MonitoringService.MIN_INTERVAL)
            .build();

        monitoringService = new MonitoringService(settings, clusterService, threadPool, emptySet(), exporter);

        monitoringService.start();
        assertBusy(() -> assertTrue(monitoringService.isStarted()));

        assertBusy(() -> assertThat(exporter.getExportsCount(), equalTo(1)));

        monitoringService.cancelExecution();

        latch.countDown();

        assertThat(exporter.getExportsCount(), equalTo(1));
        assertWarnings(
            "[xpack.monitoring.collection.enabled] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[xpack.monitoring.collection.interval] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    class CountingExporter extends Exporters {

        private final AtomicInteger exports = new AtomicInteger(0);

        CountingExporter() {
            super(Settings.EMPTY, Collections.emptyMap(), clusterService, licenseState, threadPool.getThreadContext(), sslService);
        }

        @Override
        public void export(Collection<MonitoringDoc> docs, ActionListener<Void> listener) {
            exports.incrementAndGet();
            listener.onResponse(null);
        }

        int getExportsCount() {
            return exports.get();
        }

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}

        @Override
        protected void doClose() {}
    }

    class BlockingExporter extends CountingExporter {

        private final CountDownLatch latch;

        BlockingExporter(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void export(Collection<MonitoringDoc> docs, ActionListener<Void> listener) {
            super.export(docs, listener.delegateFailureAndWrap((l, r) -> {
                try {
                    latch.await();
                    l.onResponse(null);
                } catch (InterruptedException e) {
                    l.onFailure(new ExportException("BlockingExporter failed", e));
                }
            }));

        }

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}

        @Override
        protected void doClose() {}
    }
}
