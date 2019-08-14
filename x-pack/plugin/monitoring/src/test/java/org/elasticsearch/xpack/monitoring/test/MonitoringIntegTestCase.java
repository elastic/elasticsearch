/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.test;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.core.monitoring.test.MockPainlessScriptEngine;
import org.elasticsearch.xpack.monitoring.LocalStateMonitoring;
import org.elasticsearch.xpack.monitoring.MonitoringService;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public abstract class MonitoringIntegTestCase extends ESIntegTestCase {

    protected static final String MONITORING_INDICES_PREFIX = ".monitoring-";
    protected static final String ALL_MONITORING_INDICES = MONITORING_INDICES_PREFIX + "*";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringService.INTERVAL.getKey(), MonitoringService.MIN_INTERVAL)
//                .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
//                .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
                // Disable native ML autodetect_process as the c++ controller won't be available
//                .put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false)
//                .put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false)
                // we do this by default in core, but for monitoring this isn't needed and only adds noise.
                .put("index.store.mock.check_index_on_close", false);

        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Set<Class<? extends Plugin>> plugins = new HashSet<>(super.getMockPlugins());
        plugins.remove(MockTransportService.TestPlugin.class); // security has its own transport service
        plugins.add(MockFSIndexStore.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateMonitoring.class, MockPainlessScriptEngine.TestPlugin.class,
                MockIngestPlugin.class, CommonAnalysisPlugin.class);
    }

    @Override
    protected Set<String> excludeTemplates() {
        return new HashSet<>(monitoringTemplateNames());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        startMonitoringService();
    }

    @After
    public void tearDown() throws Exception {
        stopMonitoringService();
        super.tearDown();
    }

    protected void startMonitoringService() {
        internalCluster().getInstances(MonitoringService.class).forEach(MonitoringService::start);
    }

    protected void stopMonitoringService() {
        internalCluster().getInstances(MonitoringService.class).forEach(MonitoringService::stop);
    }

    protected void wipeMonitoringIndices() throws Exception {
        CountDown retries = new CountDown(3);
        assertBusy(() -> {
            try {
                if (indexExists(ALL_MONITORING_INDICES)) {
                    deleteMonitoringIndices();
                } else {
                    retries.countDown();
                }
            } catch (IndexNotFoundException e) {
                retries.countDown();
            }
            assertThat(retries.isCountedDown(), is(true));
        });
    }

    protected void deleteMonitoringIndices() {
        assertAcked(client().admin().indices().prepareDelete(ALL_MONITORING_INDICES));
    }

    protected void ensureMonitoringIndicesYellow() {
        ensureYellowAndNoInitializingShards(".monitoring-es-*");
    }

    protected List<Tuple<String, String>> monitoringTemplates() {
        return Arrays.stream(MonitoringTemplateUtils.TEMPLATE_IDS)
                    .map(id -> new Tuple<>(MonitoringTemplateUtils.templateName(id), MonitoringTemplateUtils.loadTemplate(id)))
                    .collect(Collectors.toList());
    }

    protected List<String> monitoringTemplateNames() {
        return Arrays.stream(MonitoringTemplateUtils.TEMPLATE_IDS)
                    .map(MonitoringTemplateUtils::templateName)
                    .collect(Collectors.toList());
    }

    private Tuple<String, String> monitoringPipeline(final String pipelineId) {
        final XContentType json = XContentType.JSON;

        return new Tuple<>(MonitoringTemplateUtils.pipelineName(pipelineId),
                Strings.toString(MonitoringTemplateUtils.loadPipeline(pipelineId, json)));
    }

    protected List<Tuple<String, String>> monitoringPipelines() {
        return Arrays.stream(MonitoringTemplateUtils.PIPELINE_IDS)
                .map(this::monitoringPipeline)
                .collect(Collectors.toList());
    }

    protected List<String> monitoringPipelineNames() {
        return Arrays.stream(MonitoringTemplateUtils.PIPELINE_IDS)
                .map(MonitoringTemplateUtils::pipelineName)
                .collect(Collectors.toList());
    }

    protected List<Tuple<String, String>> monitoringWatches() {
        final ClusterService clusterService = clusterService();

        return Arrays.stream(ClusterAlertsUtil.WATCH_IDS)
                .map(id -> new Tuple<>(id, ClusterAlertsUtil.loadWatch(clusterService, id)))
                .collect(Collectors.toList());
    }

    protected void assertTemplateInstalled(String name) {
        boolean found = false;
        for (IndexTemplateMetaData template : client().admin().indices().prepareGetTemplates().get().getIndexTemplates()) {
            if (Regex.simpleMatch(name, template.getName())) {
                found =  true;
            }
        }
        assertTrue("failed to find a template matching [" + name + "]", found);
    }

    protected void waitForMonitoringIndices() throws Exception {
        awaitIndexExists(ALL_MONITORING_INDICES);
        assertBusy(this::ensureMonitoringIndicesYellow);
    }

    private void awaitIndexExists(final String index) throws Exception {
        assertBusy(() -> {
            assertIndicesExists(index);
        }, 30, TimeUnit.SECONDS);
    }

    private void assertIndicesExists(String... indices) {
        logger.trace("checking if index exists [{}]", Strings.arrayToCommaDelimitedString(indices));
        for (String index : indices) {
            assertThat(indexExists(index), is(true));
        }
    }

    protected void enableMonitoringCollection() {
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                    Settings.builder().put(MonitoringService.ENABLED.getKey(), true)));
    }

    protected void disableMonitoringCollection() {
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                    Settings.builder().putNull(MonitoringService.ENABLED.getKey())));
    }
}
