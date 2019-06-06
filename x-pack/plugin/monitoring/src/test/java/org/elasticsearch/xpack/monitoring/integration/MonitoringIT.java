/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.integration;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.License;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkRequestBuilder;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkResponse;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.LocalStateMonitoring;
import org.elasticsearch.xpack.monitoring.MonitoringService;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.indices.IndicesStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.node.NodeStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.shards.ShardMonitoringDoc;
import org.elasticsearch.xpack.monitoring.test.MockIngestPlugin;

import java.io.IOException;
import java.lang.Thread.State;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.threadpool.ThreadPool.Names.WRITE;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.TEMPLATE_VERSION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MonitoringIT extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
                       .put(super.nodeSettings())
                       .put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false)
                       .put("xpack.monitoring.collection.interval", MonitoringService.MIN_INTERVAL)
                       .put("xpack.monitoring.exporters._local.type", "local")
                       .put("xpack.monitoring.exporters._local.enabled", false)
                       .put("xpack.monitoring.exporters._local.cluster_alerts.management.enabled", false)
                       .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateMonitoring.class, MockIngestPlugin.class, CommonAnalysisPlugin.class);
    }

    private String createBulkEntity() {
        return  "{\"index\":{\"_type\":\"monitoring_data_type\"}}\n" +
                "{\"foo\":{\"bar\":0}}\n" +
                "{\"index\":{\"_type\":\"monitoring_data_type\"}}\n" +
                "{\"foo\":{\"bar\":1}}\n" +
                "{\"index\":{\"_type\":\"monitoring_data_type\"}}\n" +
                "{\"foo\":{\"bar\":2}}\n" +
                "\n";
    }

    /**
     * Monitoring Bulk test:
     *
     * This test uses the Monitoring Bulk Request to index documents. It then ensure that the documents were correctly
     * indexed and have the expected information. REST API tests (like how this is really called) are handled as part of the
     * XPackRest tests.
     */
    public void testMonitoringBulk() throws Exception {
        whenExportersAreReady(() -> {
            final MonitoredSystem system = randomSystem();
            final TimeValue interval = TimeValue.timeValueSeconds(randomIntBetween(1, 20));

            final MonitoringBulkResponse bulkResponse =
                    new MonitoringBulkRequestBuilder(client())
                            .add(system, new BytesArray(createBulkEntity().getBytes("UTF-8")), XContentType.JSON,
                                 System.currentTimeMillis(), interval.millis())
                    .get();

            assertThat(bulkResponse.status(), is(RestStatus.OK));
            assertThat(bulkResponse.getError(), nullValue());

            final String monitoringIndex = ".monitoring-" + system.getSystem() + "-" + TEMPLATE_VERSION + "-*";

            // Wait for the monitoring index to be created
            assertBusy(() -> {
                // Monitoring uses auto_expand_replicas, so it should be green even without replicas
                ensureGreen(monitoringIndex);
                assertThat(client().admin().indices().prepareRefresh(monitoringIndex).get().getStatus(), is(RestStatus.OK));

                final SearchResponse response =
                        client().prepareSearch(".monitoring-" + system.getSystem() + "-" + TEMPLATE_VERSION + "-*")
                                .get();

                // exactly 3 results are expected
                assertThat("No monitoring documents yet", response.getHits().getTotalHits().value, equalTo(3L));

                final List<Map<String, Object>> sources =
                        Arrays.stream(response.getHits().getHits())
                              .map(SearchHit::getSourceAsMap)
                              .collect(Collectors.toList());

                // find distinct _source.timestamp fields
                assertThat(sources.stream().map(source -> source.get("timestamp")).distinct().count(), is(1L));
                // find distinct _source.source_node fields (which is a map)
                assertThat(sources.stream().map(source -> source.get("source_node")).distinct().count(), is(1L));
            });

            final SearchResponse response = client().prepareSearch(monitoringIndex).get();
            final SearchHits hits = response.getHits();

            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat("Monitoring documents must have the same timestamp",
                       Arrays.stream(hits.getHits())
                             .map(hit -> extractValue("timestamp", hit.getSourceAsMap()))
                             .distinct()
                             .count(),
                       equalTo(1L));
            assertThat("Monitoring documents must have the same source_node timestamp",
                       Arrays.stream(hits.getHits())
                             .map(hit -> extractValue("source_node.timestamp", hit.getSourceAsMap()))
                             .distinct()
                             .count(),
                       equalTo(1L));

            for (final SearchHit hit : hits.getHits()) {
                assertMonitoringDoc(toMap(hit), system, "monitoring_data_type", interval);
            }
        });
    }

    /**
     * Monitoring Service test:
     *
     * This test waits for the monitoring service to collect monitoring documents and then checks that all expected documents
     * have been indexed with the expected information.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/29880")
    public void testMonitoringService() throws Exception {
        final boolean createAPMIndex = randomBoolean();
        final String indexName = createAPMIndex ? "apm-2017.11.06" : "books";

        assertThat(client().prepareIndex(indexName, "doc", "0")
                           .setRefreshPolicy("true")
                           .setSource("{\"field\":\"value\"}", XContentType.JSON)
                           .get()
                           .status(),
                   is(RestStatus.CREATED));

        final Settings settings = Settings.builder()
            .put("cluster.metadata.display_name", "my cluster")
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        whenExportersAreReady(() -> {
            final AtomicReference<SearchResponse> searchResponse = new AtomicReference<>();

            assertBusy(() -> {
                final SearchResponse response =
                        client().prepareSearch(".monitoring-es-*")
                                .setCollapse(new CollapseBuilder("type"))
                                .addSort("timestamp", SortOrder.DESC)
                                .get();

                assertThat(response.status(), is(RestStatus.OK));
                assertThat("Expecting a minimum number of 6 docs, one per collector",
                           response.getHits().getHits().length,
                           greaterThanOrEqualTo(6));

                searchResponse.set(response);
            });

            for (final SearchHit hit : searchResponse.get().getHits()) {
                final Map<String, Object> searchHit = toMap(hit);
                final String type = (String) extractValue("_source.type", searchHit);

                assertMonitoringDoc(searchHit, MonitoredSystem.ES, type, MonitoringService.MIN_INTERVAL);

                if (ClusterStatsMonitoringDoc.TYPE.equals(type)) {
                    assertClusterStatsMonitoringDoc(searchHit, createAPMIndex);
                } else if (IndexRecoveryMonitoringDoc.TYPE.equals(type)) {
                    assertIndexRecoveryMonitoringDoc(searchHit);
                } else if (IndicesStatsMonitoringDoc.TYPE.equals(type)) {
                    assertIndicesStatsMonitoringDoc(searchHit);
                } else if (IndexStatsMonitoringDoc.TYPE.equals(type)) {
                    assertIndexStatsMonitoringDoc(searchHit);
                } else if (NodeStatsMonitoringDoc.TYPE.equals(type)) {
                    assertNodeStatsMonitoringDoc(searchHit);
                } else if (ShardMonitoringDoc.TYPE.equals(type)) {
                    assertShardMonitoringDoc(searchHit);
                } else {
                    fail("Monitoring document of type [" + type + "] is not supported by this test");
                }
            }
        });

    }

    /**
     * Asserts that the monitoring document (provided as a Map) contains the common information that
     * all monitoring documents must have
     */
    @SuppressWarnings("unchecked")
    private void assertMonitoringDoc(final Map<String, Object> document,
                                     final MonitoredSystem expectedSystem,
                                     final String expectedType,
                                     final TimeValue interval) {
        assertEquals(document.toString(),4, document.size());

        final String index = (String) document.get("_index");
        assertThat(index, containsString(".monitoring-" + expectedSystem.getSystem() + "-" + TEMPLATE_VERSION + "-"));
        assertThat((String) document.get("_id"), not(isEmptyOrNullString()));

        final Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertThat(source, notNullValue());
        assertThat((String) source.get("cluster_uuid"), not(isEmptyOrNullString()));
        assertThat(source.get("type"), equalTo(expectedType));

        final String timestamp = (String) source.get("timestamp");
        assertThat(timestamp, not(isEmptyOrNullString()));

        assertThat(((Number) source.get("interval_ms")).longValue(), equalTo(interval.getMillis()));

        DateFormatter formatter = DateFormatter.forPattern("yyyy.MM.dd");
        long isoTimestamp = Instant.from(DateFormatter.forPattern("strict_date_time").parse(timestamp)).toEpochMilli();
        String isoDateTime = MonitoringTemplateUtils.indexName(formatter.withZone(ZoneOffset.UTC), expectedSystem, isoTimestamp);
        assertThat(index, equalTo(isoDateTime));

        final Map<String, Object> sourceNode = (Map<String, Object>) source.get("source_node");
        if (sourceNode != null) {
            assertMonitoringDocSourceNode(sourceNode);
        }
    }

    /**
     * Asserts that the source_node information (provided as a Map) of a monitoring document correspond to
     * the current local node information
     */
    private void assertMonitoringDocSourceNode(final Map<String, Object> sourceNode) {
        assertEquals(6, sourceNode.size());

        final NodesInfoResponse nodesResponse = client().admin().cluster().prepareNodesInfo().clear().get();

        assertEquals(1, nodesResponse.getNodes().size());

        final DiscoveryNode node = nodesResponse.getNodes().stream().findFirst().get().getNode();

        assertThat(sourceNode.get("uuid"), equalTo(node.getId()));
        assertThat(sourceNode.get("host"), equalTo(node.getHostName()));
        assertThat(sourceNode.get("transport_address"),equalTo(node.getAddress().toString()));
        assertThat(sourceNode.get("ip"), equalTo(node.getAddress().getAddress()));
        assertThat(sourceNode.get("name"), equalTo(node.getName()));
        assertThat((String) sourceNode.get("timestamp"), not(isEmptyOrNullString()));
    }

    /**
     * Assert that a {@link ClusterStatsMonitoringDoc} contains the expected information
     */
    @SuppressWarnings("unchecked")
    private void assertClusterStatsMonitoringDoc(final Map<String, Object> document,
                                                 final boolean apmIndicesExist) {
        final Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertEquals(12, source.size());

        assertThat((String) source.get("cluster_name"), not(isEmptyOrNullString()));
        assertThat(source.get("version"), equalTo(Version.CURRENT.toString()));

        final Map<String, Object> license = (Map<String, Object>) source.get("license");
        assertThat(license, notNullValue());
        assertThat((String) license.get(License.Fields.ISSUER), not(isEmptyOrNullString()));
        assertThat((String) license.get(License.Fields.ISSUED_TO), not(isEmptyOrNullString()));
        assertThat((Long) license.get(License.Fields.ISSUE_DATE_IN_MILLIS), greaterThan(0L));
        assertThat((Integer) license.get(License.Fields.MAX_NODES), greaterThan(0));

        String uid = (String) license.get("uid");
        assertThat(uid, not(isEmptyOrNullString()));

        String type = (String) license.get("type");
        assertThat(type, not(isEmptyOrNullString()));

        String status = (String) license.get(License.Fields.STATUS);
        assertThat(status, not(isEmptyOrNullString()));

        if ("basic".equals(license.get("type")) == false) {
            Long expiryDate = (Long) license.get(License.Fields.EXPIRY_DATE_IN_MILLIS);
            assertThat(expiryDate, greaterThan(0L));
        }

        Boolean clusterNeedsTLS = (Boolean) license.get("cluster_needs_tls");
        assertThat(clusterNeedsTLS, isOneOf(true, null));

        final Map<String, Object> clusterStats = (Map<String, Object>) source.get("cluster_stats");
        assertThat(clusterStats, notNullValue());
        assertThat(clusterStats.size(), equalTo(5));

        final Map<String, Object> stackStats = (Map<String, Object>) source.get("stack_stats");
        assertThat(stackStats, notNullValue());
        assertThat(stackStats.size(), equalTo(2));

        final Map<String, Object> apm = (Map<String, Object>) stackStats.get("apm");
        assertThat(apm, notNullValue());
        assertThat(apm.size(), equalTo(1));
        assertThat(apm.remove("found"), is(apmIndicesExist));
        assertThat(apm.keySet(), empty());

        final Map<String, Object> xpackStats = (Map<String, Object>) stackStats.get("xpack");
        assertThat(xpackStats, notNullValue());
        assertThat("X-Pack stats must have at least monitoring, but others may be hidden", xpackStats.size(), greaterThanOrEqualTo(1));

        final Map<String, Object> monitoring = (Map<String, Object>) xpackStats.get("monitoring");
        // we don't make any assumptions about what's in it, only that it's there
        assertThat(monitoring, notNullValue());

        final Map<String, Object> clusterState = (Map<String, Object>) source.get("cluster_state");
        assertThat(clusterState, notNullValue());
        assertThat(clusterState.remove("nodes_hash"), notNullValue());
        assertThat(clusterState.remove("status"), notNullValue());
        assertThat(clusterState.remove("version"), notNullValue());
        assertThat(clusterState.remove("state_uuid"), notNullValue());
        assertThat(clusterState.remove("cluster_uuid"), notNullValue());
        assertThat(clusterState.remove("master_node"), notNullValue());
        assertThat(clusterState.remove("nodes"), notNullValue());
        assertThat(clusterState.keySet(), empty());

        final Map<String, Object> clusterSettings = (Map<String, Object>) source.get("cluster_settings");
        assertThat(clusterSettings, notNullValue());
        assertThat(clusterSettings.remove("cluster"), notNullValue());
        assertThat(clusterSettings.keySet(), empty());
    }

    /**
     * Assert that a {@link IndexRecoveryMonitoringDoc} contains the expected information
     */
    @SuppressWarnings("unchecked")
    private void assertIndexRecoveryMonitoringDoc(final Map<String, Object> document) {
        final Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertEquals(6, source.size());

        final Map<String, Object> indexRecovery = (Map<String, Object>) source.get(IndexRecoveryMonitoringDoc.TYPE);
        assertEquals(1, indexRecovery.size());

        final List<Object> shards = (List<Object>) indexRecovery.get("shards");
        assertThat(shards, notNullValue());
    }

    /**
     * Assert that a {@link IndicesStatsMonitoringDoc} contains the expected information
     */
    @SuppressWarnings("unchecked")
    private void assertIndicesStatsMonitoringDoc(final Map<String, Object> document) {
        final Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertEquals(6, source.size());

        final Map<String, Object> indicesStats = (Map<String, Object>) source.get(IndicesStatsMonitoringDoc.TYPE);
        assertEquals(1, indicesStats.size());

        IndicesStatsMonitoringDoc.XCONTENT_FILTERS.forEach(filter ->
                assertThat(filter + " must not be null in the monitoring document", extractValue(filter, source), notNullValue()));
    }

    /**
     * Assert that a {@link IndexStatsMonitoringDoc} contains the expected information
     */
    @SuppressWarnings("unchecked")
    private void assertIndexStatsMonitoringDoc(final Map<String, Object> document) {
        final Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertEquals(6, source.size());

        // particular field values checked in the index stats tests
        final Map<String, Object> indexStats = (Map<String, Object>) source.get(IndexStatsMonitoringDoc.TYPE);
        assertEquals(7, indexStats.size());
        assertThat((String) indexStats.get("index"), not(isEmptyOrNullString()));
        assertThat((String) indexStats.get("uuid"), not(isEmptyOrNullString()));
        assertThat(indexStats.get("created"), notNullValue());
        assertThat((String) indexStats.get("status"), not(isEmptyOrNullString()));
        assertThat(indexStats.get("shards"), notNullValue());
        final Map<String, Object> shards = (Map<String, Object>) indexStats.get("shards");
        assertEquals(11, shards.size());
        assertThat(indexStats.get("primaries"), notNullValue());
        assertThat(indexStats.get("total"), notNullValue());

        IndexStatsMonitoringDoc.XCONTENT_FILTERS.forEach(filter ->
                assertThat(filter + " must not be null in the monitoring document", extractValue(filter, source), notNullValue()));
    }

    /**
     * Assert that a {@link NodeStatsMonitoringDoc} contains the expected information
     */
    @SuppressWarnings("unchecked")
    private void assertNodeStatsMonitoringDoc(final Map<String, Object> document) {
        final Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertEquals(6, source.size());

        NodeStatsMonitoringDoc.XCONTENT_FILTERS.forEach(filter -> {
            if (Constants.WINDOWS && filter.startsWith("node_stats.os.cpu.load_average")) {
                // load average is unavailable on Windows
                return;
            }

            // fs and cgroup stats are only reported on Linux, but it's acceptable for _node/stats to report them as null if the OS is
            //  misconfigured or not reporting them for some reason (e.g., older kernel)
            if (filter.startsWith("node_stats.fs") || filter.startsWith("node_stats.os.cgroup")) {
                return;
            }

            // load average is unavailable on macOS for 5m and 15m (but we get 1m), but it's also possible on Linux too
            if ("node_stats.os.cpu.load_average.5m".equals(filter) || "node_stats.os.cpu.load_average.15m".equals(filter)) {
                return;
            }

            // bulk is not a thread pool in the current version but we allow it to support mixed version clusters
            if (filter.startsWith("node_stats.thread_pool.bulk")) {
                return;
            }

            assertThat(filter + " must not be null in the monitoring document", extractValue(filter, source), notNullValue());
        });
    }

    /**
     * Assert that a {@link ShardMonitoringDoc} contains the expected information
     */
    @SuppressWarnings("unchecked")
    private void assertShardMonitoringDoc(final Map<String, Object> document) {
        final Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertEquals(7, source.size());
        assertThat(source.get("state_uuid"), notNullValue());

        final Map<String, Object> shard = (Map<String, Object>) source.get("shard");
        assertEquals(6, shard.size());

        final String currentNodeId = (String) shard.get("node");
        if (Strings.hasLength(currentNodeId)) {
            assertThat(source.get("source_node"), notNullValue());
        } else {
            assertThat(source.get("source_node"), nullValue());
        }

        ShardMonitoringDoc.XCONTENT_FILTERS.forEach(filter -> {
            if (filter.equals("shard.relocating_node")) {
                // Shard's relocating node is null most of the time in this test, we only check that the field is here
                assertTrue(filter + " must exist in the monitoring document", shard.containsKey("relocating_node"));
                return;
            }
            if (filter.equals("shard.node")) {
                // Current node is null for replicas in this test, we only check that the field is here
                assertTrue(filter + " must exist in the monitoring document", shard.containsKey("node"));
                return;
            }
            assertThat(filter + " must not be null in the monitoring document", extractValue(filter, source), notNullValue());
        });
    }

    /**
     * Executes the given {@link Runnable} once the monitoring exporters are ready and functional. Ensure that
     * the exporters and the monitoring service are shut down after the runnable has been executed.
     */
    private void whenExportersAreReady(final CheckedRunnable<Exception> runnable) throws Exception {
        try {
            try {
                enableMonitoring();
            } catch (AssertionError e) {
                // Added to debug https://github.com/elastic/elasticsearch/issues/29880
                // Remove when fixed
                StringBuilder b = new StringBuilder();
                b.append("\n==== jstack at monitoring enablement failure time ====\n");
                for (ThreadInfo ti : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
                  append(b, ti);
                }
                b.append("^^==============================================\n");
                logger.info(b.toString());
                throw e;
            }
            runnable.run();
        } finally {
            disableMonitoring();
        }
    }

    // borrowed from randomized-testing
    private static void append(StringBuilder b, ThreadInfo ti) {
        b.append('"').append(ti.getThreadName()).append('"');
        b.append(" ID=").append(ti.getThreadId());

        final State threadState = ti.getThreadState();
        b.append(" ").append(threadState);
        if (ti.getLockName() != null) {
          b.append(" on ").append(ti.getLockName());
        }

        if (ti.getLockOwnerName() != null) {
          b.append(" owned by \"").append(ti.getLockOwnerName())
           .append("\" ID=").append(ti.getLockOwnerId());
        }

        b.append(ti.isSuspended() ? " (suspended)" : "");
        b.append(ti.isInNative() ? " (in native code)" : "");
        b.append("\n");

        final StackTraceElement[] stack = ti.getStackTrace();
        final LockInfo lockInfo = ti.getLockInfo();
        final MonitorInfo [] monitorInfos = ti.getLockedMonitors();
        for (int i = 0; i < stack.length; i++) {
          b.append("\tat ").append(stack[i]).append("\n");
          if (i == 0 && lockInfo != null) {
            b.append("\t- ")
             .append(threadState)
             .append(lockInfo)
             .append("\n");
          }

          for (MonitorInfo mi : monitorInfos) {
            if (mi.getLockedStackDepth() == i) {
              b.append("\t- locked ").append(mi).append("\n");
            }
          }
        }

        LockInfo [] lockInfos = ti.getLockedSynchronizers();
        if (lockInfos.length > 0) {
          b.append("\tLocked synchronizers:\n");
          for (LockInfo li : ti.getLockedSynchronizers()) {
            b.append("\t- ").append(li).append("\n");
          }
        }
        b.append("\n");
      }

    /**
     * Enable the monitoring service and the Local exporter, waiting for some monitoring documents
     * to be indexed before it returns.
     */
    public void enableMonitoring() throws Exception {
        // delete anything that may happen to already exist
        assertAcked(client().admin().indices().prepareDelete(".monitoring-*"));

        assertThat("Must be no enabled exporters before enabling monitoring", getMonitoringUsageExportersDefined(), is(false));

        final Settings settings = Settings.builder()
                .put("xpack.monitoring.collection.enabled", true)
                .put("xpack.monitoring.exporters._local.enabled", true)
                .build();

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        assertBusy(() -> assertThat("[_local] exporter not enabled yet", getMonitoringUsageExportersDefined(), is(true)));

        assertBusy(() -> {
            // Monitoring uses auto_expand_replicas, so it should be green even without replicas
            ensureGreen(".monitoring-es-*");
            assertThat(client().admin().indices().prepareRefresh(".monitoring-es-*").get().getStatus(), is(RestStatus.OK));

            assertThat("No monitoring documents yet",
                       client().prepareSearch(".monitoring-es-" + TEMPLATE_VERSION + "-*")
                               .setSize(0)
                               .get().getHits().getTotalHits().value,
                       greaterThan(0L));
        }, 30L, TimeUnit.SECONDS);
    }

    /**
     * Disable the monitoring service and the Local exporter.
     */
    public void disableMonitoring() throws Exception {
        final Settings settings = Settings.builder()
                .putNull("xpack.monitoring.collection.enabled")
                .putNull("xpack.monitoring.exporters._local.enabled")
                .putNull("cluster.metadata.display_name")
                .build();

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        assertBusy(() -> assertThat("Exporters are not yet stopped", getMonitoringUsageExportersDefined(), is(false)));
        assertBusy(() -> {
            try {
                // now wait until Monitoring has actually stopped
                final NodesStatsResponse response = client().admin().cluster().prepareNodesStats().clear().setThreadPool(true).get();

                for (final NodeStats nodeStats : response.getNodes()) {
                    boolean foundBulkThreads = false;

                    for(final ThreadPoolStats.Stats threadPoolStats : nodeStats.getThreadPool()) {
                        if (WRITE.equals(threadPoolStats.getName())) {
                            foundBulkThreads = true;
                            assertThat("Still some active _bulk threads!", threadPoolStats.getActive(), equalTo(0));
                            break;
                        }
                    }

                    assertThat("Could not find bulk thread pool", foundBulkThreads, is(true));
                }
            } catch (Exception e) {
                throw new ElasticsearchException("Failed to wait for monitoring exporters to stop:", e);
            }
        }, 30L, TimeUnit.SECONDS);
    }

    private boolean getMonitoringUsageExportersDefined() throws Exception {
        final XPackUsageResponse usageResponse = new XPackUsageRequestBuilder(client()).execute().get();
        final Optional<MonitoringFeatureSetUsage> monitoringUsage =
                usageResponse.getUsages()
                        .stream()
                        .filter(usage -> usage instanceof MonitoringFeatureSetUsage)
                        .map(usage -> (MonitoringFeatureSetUsage)usage)
                        .findFirst();

        assertThat("Monitoring feature set does not exist", monitoringUsage.isPresent(), is(true));

        return monitoringUsage.get().getExporters().isEmpty() == false;
    }

    /**
     * Returns the {@link SearchHit} content as a {@link Map} object.
     */
    private static Map<String, Object> toMap(final ToXContentObject xContentObject) throws IOException {
        final XContentType xContentType = XContentType.JSON;

        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            xContentObject.toXContent(builder, EMPTY_PARAMS);

            final Map<String, Object> map = XContentHelper.convertToMap(xContentType.xContent(), Strings.toString(builder), false);

            // remove extraneous fields not actually wanted from the response
            map.remove("_score");
            map.remove("fields");
            map.remove("sort");

            return map;
        }
    }

    /**
     * Returns a {@link MonitoredSystem} supported by the Monitoring Bulk API
     */
    private static MonitoredSystem randomSystem() {
        return randomFrom(MonitoredSystem.LOGSTASH, MonitoredSystem.KIBANA, MonitoredSystem.BEATS);
    }
}
