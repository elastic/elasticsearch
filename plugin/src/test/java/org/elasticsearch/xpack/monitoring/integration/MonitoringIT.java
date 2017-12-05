/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.integration;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.license.License;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.indices.IndicesStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.node.NodeStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.shards.ShardMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.rest.action.RestMonitoringBulkAction;
import org.hamcrest.Matcher;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsMonitoringDoc.hash;
import static org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils.TEMPLATE_VERSION;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MonitoringIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("x_pack_rest_user", TEST_PASSWORD_SECURE_STRING);

    private final TimeValue collectionInterval = TimeValue.timeValueSeconds(3);

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
                       .put(super.restClientSettings())
                       .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)
                       .build();
    }

    private HttpEntity createBulkEntity() {
        final StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{\"_type\":\"test\"}}\n");
        bulk.append("{\"foo\":{\"bar\":0}}\n");
        bulk.append("{\"index\":{\"_type\":\"test\"}}\n");
        bulk.append("{\"foo\":{\"bar\":1}}\n");
        bulk.append("{\"index\":{\"_type\":\"test\"}}\n");
        bulk.append("{\"foo\":{\"bar\":2}}\n");
        bulk.append("\n");
        return new NStringEntity(bulk.toString(), ContentType.APPLICATION_JSON);
    }

    public void testMonitoringBulkApiWithMissingSystemId() throws IOException {
        final Map<String, String> parameters = parameters(null, TEMPLATE_VERSION, "10s");

        assertBadRequest(parameters, createBulkEntity(), containsString("no [system_id] for monitoring bulk request"));
    }

    public void testMonitoringBulkApiWithMissingSystemApiVersion() throws IOException {
        final Map<String, String> parameters = parameters(randomSystemId(), null, "10s");

        assertBadRequest(parameters, createBulkEntity(), containsString("no [system_api_version] for monitoring bulk request"));
    }

    public void testMonitoringBulkApiWithMissingInterval() throws IOException {
        final Map<String, String> parameters = parameters(randomSystemId(), TEMPLATE_VERSION, null);

        assertBadRequest(parameters, createBulkEntity(), containsString("no [interval] for monitoring bulk request"));
    }

    public void testMonitoringBulkApiWithWrongInterval() throws IOException {
        final Map<String, String> parameters = parameters(randomSystemId(), TEMPLATE_VERSION, "null");

        assertBadRequest(parameters, createBulkEntity(), containsString("failed to parse setting [interval] with value [null]"));
    }

    public void testMonitoringBulkApiWithMissingContent() throws IOException {
        final Map<String, String> parameters = parameters(randomSystemId(), TEMPLATE_VERSION, "30s");

        assertBadRequest(parameters, null, containsString("no body content for monitoring bulk request"));
    }

    public void testMonitoringBulkApiWithUnsupportedSystemVersion() throws IOException {
        final String systemId = randomSystemId();
        final String systemApiVersion = randomFrom(TEMPLATE_VERSION, MonitoringTemplateUtils.OLD_TEMPLATE_VERSION);

        Map<String, String> parameters = parameters(MonitoredSystem.UNKNOWN.getSystem(), systemApiVersion, "30s");
        assertBadRequest(parameters, createBulkEntity(),
                containsString("system_api_version ["+ systemApiVersion + "] is not supported by system_id [unknown]"));

        parameters = parameters(systemId, "0", "30s");
        assertBadRequest(parameters, createBulkEntity(),
                containsString("system_api_version [0] is not supported by system_id [" + systemId + "]"));
    }

    /**
     * Monitoring Bulk API test:
     *
     * This test uses the Monitoring Bulk API to index document as an external application like Kibana would do. It
     * then ensure that the documents were correctly indexed and have the expected information.
     */
    @SuppressWarnings("unchecked")
    public void testMonitoringBulk() throws Exception {
        whenExportersAreReady(() -> {
            final MonitoredSystem system = randomSystem();
            final TimeValue interval = TimeValue.timeValueSeconds(randomIntBetween(1, 20));

            // Use Monitoring Bulk API to index 3 documents
            Response bulkResponse = client().performRequest("POST", "/_xpack/monitoring/_bulk",
                                                            parameters(system.getSystem(), TEMPLATE_VERSION, interval.getStringRep()),
                                                            createBulkEntity());

            assertThat(bulkResponse.getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(toMap(bulkResponse.getEntity()).get("errors"), equalTo(false));

            final String indexPrefix = ".monitoring-" + system.getSystem() + "-" + TEMPLATE_VERSION;

            // Wait for the monitoring index to be created
            awaitRestApi("GET", "/" + indexPrefix + "-*/_search", singletonMap("size", "0"), null,
                    resp -> {
                        Number hitsTotal = (Number) extractValue("hits.total", resp);
                        return hitsTotal != null && hitsTotal.intValue() >= 3;
                    }, "Exception when waiting for monitoring bulk documents to be indexed");

            Response searchResponse = client().performRequest("GET", "/" + indexPrefix + "-*/_search");
            final Map<String, Object> results = toMap(searchResponse.getEntity());

            final Map<String, Object> hits = (Map<String, Object>) results.get("hits");
            final List<Map<String, Object>> searchHits = (List<Map<String,Object>>) hits.get("hits");
            assertEquals(3, searchHits.size());

            assertEquals("Monitoring documents must have the same timestamp",
                    1, searchHits.stream().map(map -> extractValue("_source.timestamp", map)).distinct().count());

            assertEquals("Monitoring documents must have the same source_node timestamp",
                    1, searchHits.stream().map(map -> extractValue("_source.source_node.timestamp", map)).distinct().count());

            for (Map<String,Object> searchHit : searchHits) {
                assertMonitoringDoc(searchHit, system, "test", interval);
            }
        });
    }

    /**
     * Monitoring Service test:
     *
     * This test waits for the monitoring service to collect monitoring documents and then checks that all expected documents
     * have been indexed with the expected information.
     */
    @SuppressWarnings("unchecked")
    public void testMonitoringService() throws Exception {
        final boolean createAPMIndex = randomBoolean();
        final String indexName = createAPMIndex ? "apm-2017.11.06" : "books";
        final HttpEntity document = new StringEntity("{\"field\":\"value\"}", ContentType.APPLICATION_JSON);
        assertThat(client().performRequest("POST", "/" + indexName + "/doc/0", singletonMap("refresh", "true"), document)
                           .getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_CREATED));

        whenExportersAreReady(() -> {
            final Response searchResponse = client().performRequest("GET", "/.monitoring-es-*/_search", singletonMap("size", "100"));
            assertThat(searchResponse.getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_OK));

            final Map<String, Object> results = toMap(searchResponse.getEntity());

            final Map<String, Object> hits = (Map<String, Object>) results.get("hits");
            assertThat("Expecting a minimum number of 6 docs, one per collector", (Integer) hits.get("total"), greaterThanOrEqualTo(6));

            final List<Map<String, Object>> searchHits = (List<Map<String,Object>>) hits.get("hits");

            for (Map<String,Object> searchHit : searchHits) {
                final String type = (String) extractValue("_source.type", searchHit);

                if (ClusterStatsMonitoringDoc.TYPE.equals(type)) {
                    assertClusterStatsMonitoringDoc(searchHit, collectionInterval, createAPMIndex);
                } else if (IndexRecoveryMonitoringDoc.TYPE.equals(type)) {
                    assertIndexRecoveryMonitoringDoc(searchHit, collectionInterval);
                } else if (IndicesStatsMonitoringDoc.TYPE.equals(type)) {
                    assertIndicesStatsMonitoringDoc(searchHit, collectionInterval);
                } else if (IndexStatsMonitoringDoc.TYPE.equals(type)) {
                    assertIndexStatsMonitoringDoc(searchHit, collectionInterval);
                } else if (NodeStatsMonitoringDoc.TYPE.equals(type)) {
                    assertNodeStatsMonitoringDoc(searchHit, collectionInterval);
                } else if (ShardMonitoringDoc.TYPE.equals(type)) {
                    assertShardMonitoringDoc(searchHit, collectionInterval);
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
    private static void assertMonitoringDoc(final Map<String, Object> document,
                                            final MonitoredSystem expectedSystem,
                                            final String expectedType,
                                            final TimeValue interval) throws Exception {
        assertEquals(5, document.size());

        final String index = (String) document.get("_index");
        assertThat(index, containsString(".monitoring-" + expectedSystem.getSystem() + "-" + TEMPLATE_VERSION + "-"));
        assertThat(document.get("_type"), equalTo("doc"));
        assertThat((String) document.get("_id"), not(isEmptyOrNullString()));

        final Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertThat(source, notNullValue());
        assertThat((String) source.get("cluster_uuid"), not(isEmptyOrNullString()));
        assertThat(source.get("type"), equalTo(expectedType));

        final String timestamp = (String) source.get("timestamp");
        assertThat(timestamp, not(isEmptyOrNullString()));

        assertThat(((Number) source.get("interval_ms")).longValue(), equalTo(interval.getMillis()));

        assertThat(index, equalTo(MonitoringTemplateUtils.indexName(DateTimeFormat.forPattern("YYYY.MM.dd").withZoneUTC(),
                                                                    expectedSystem,
                                                                    ISODateTimeFormat.dateTime().parseMillis(timestamp))));

        final Map<String, Object> sourceNode = (Map<String, Object>) source.get("source_node");
        if (sourceNode != null) {
            assertMonitoringDocSourceNode(sourceNode);
        }
    }

    /**
     * Asserts that the source_node information (provided as a Map) of a monitoring document correspond to
     * the current local node information
     */
    @SuppressWarnings("unchecked")
    private static void assertMonitoringDocSourceNode(final Map<String, Object> sourceNode) throws Exception {
        assertEquals(6, sourceNode.size());

        Map<String, String> filterPath = singletonMap("filter_path", "nodes.*.name,nodes.*.transport_address,nodes.*.host,nodes.*.ip");
        final Response nodesResponse = client().performRequest("GET", "/_nodes", filterPath);

        final Map<String, Object> nodes = (Map<String, Object>) toMap(nodesResponse.getEntity()).get("nodes");
        assertEquals(1, nodes.size());

        final String nodeId = nodes.keySet().iterator().next();

        @SuppressWarnings("unchecked")
        final Map<String, Object> node = (Map<String, Object>) nodes.get(nodeId);

        assertThat(sourceNode.get("uuid"), equalTo(nodeId));
        assertThat(sourceNode.get("host"), equalTo(node.get("host")));
        assertThat(sourceNode.get("transport_address"),equalTo(node.get("transport_address")));
        assertThat(sourceNode.get("ip"), equalTo(node.get("ip")));
        assertThat(sourceNode.get("name"), equalTo(node.get("name")));
        assertThat((String) sourceNode.get("timestamp"), not(isEmptyOrNullString()));
    }

    /**
     * Assert that a {@link ClusterStatsMonitoringDoc} contains the expected information
     */
    @SuppressWarnings("unchecked")
    private static void assertClusterStatsMonitoringDoc(final Map<String, Object> document,
                                                        final TimeValue interval,
                                                        final boolean apmIndicesExist) throws Exception {
        assertMonitoringDoc(document, MonitoredSystem.ES, ClusterStatsMonitoringDoc.TYPE, interval);

        final Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertEquals(11, source.size());

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

        Long expiryDate = (Long) license.get(License.Fields.EXPIRY_DATE_IN_MILLIS);
        assertThat(expiryDate, greaterThan(0L));

        Boolean clusterNeedsTLS = (Boolean) license.get("cluster_needs_tls");
        assertThat(clusterNeedsTLS, isOneOf(true, null));

        // We basically recompute the hash here
        assertThat("Hash key should be the same",
                license.get("hkey"), equalTo(hash(status, uid, type, String.valueOf(expiryDate), (String) source.get("cluster_uuid"))));

        final Map<String, Object> clusterStats = (Map<String, Object>) source.get("cluster_stats");
        assertThat(clusterStats, notNullValue());
        assertThat(clusterStats.size(), equalTo(4));

        final Map<String, Object> stackStats = (Map<String, Object>) source.get("stack_stats");
        assertThat(stackStats, notNullValue());
        assertThat(stackStats.size(), equalTo(2));

        final Map<String, Object> apm = (Map<String, Object>) stackStats.get("apm");
        assertThat(apm, notNullValue());
        assertThat(apm.size(), equalTo(1));
        assertThat(apm.remove("found"), is(apmIndicesExist));
        assertThat(apm.isEmpty(), is(true));

        final Map<String, Object> xpackStats = (Map<String, Object>) stackStats.get("xpack");
        assertThat(xpackStats, notNullValue());
        assertThat("X-Pack stats must have at least monitoring, but others may be hidden", xpackStats.size(), greaterThanOrEqualTo(1));

        final Map<String, Object> monitoring = (Map<String, Object>) xpackStats.get("monitoring");
        // we don't make any assumptions about what's in it, only that it's there
        assertThat(monitoring, notNullValue());

        final Map<String, Object> clusterState = (Map<String, Object>) source.get("cluster_state");
        assertThat(clusterState, notNullValue());
        assertThat(clusterState.size(), equalTo(6));
        assertThat(clusterState.remove("nodes_hash"), notNullValue());
        assertThat(clusterState.remove("status"), notNullValue());
        assertThat(clusterState.remove("version"), notNullValue());
        assertThat(clusterState.remove("state_uuid"), notNullValue());
        assertThat(clusterState.remove("master_node"), notNullValue());
        assertThat(clusterState.remove("nodes"), notNullValue());
        assertThat(clusterState.isEmpty(), is(true));
    }

    /**
     * Assert that a {@link IndexRecoveryMonitoringDoc} contains the expected information
     */
    @SuppressWarnings("unchecked")
    private static void assertIndexRecoveryMonitoringDoc(final Map<String, Object> document, final TimeValue interval) throws Exception {
        assertMonitoringDoc(document, MonitoredSystem.ES, IndexRecoveryMonitoringDoc.TYPE, interval);

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
    private static void assertIndicesStatsMonitoringDoc(final Map<String, Object> document, final TimeValue interval) throws Exception {
        assertMonitoringDoc(document, MonitoredSystem.ES, IndicesStatsMonitoringDoc.TYPE, interval);

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
    private static void assertIndexStatsMonitoringDoc(final Map<String, Object> document, final TimeValue interval) throws Exception {
        assertMonitoringDoc(document, MonitoredSystem.ES, IndexStatsMonitoringDoc.TYPE, interval);

        final Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertEquals(6, source.size());

        // particular field values checked in the index stats tests
        final Map<String, Object> indexStats = (Map<String, Object>) source.get(IndexStatsMonitoringDoc.TYPE);
        assertEquals(8, indexStats.size());
        assertThat((String) indexStats.get("index"), not(isEmptyOrNullString()));
        assertThat((String) indexStats.get("uuid"), not(isEmptyOrNullString()));
        assertThat((Long) indexStats.get("created"), notNullValue());
        assertThat((String) indexStats.get("status"), not(isEmptyOrNullString()));
        assertThat(indexStats.get("version"), notNullValue());
        final Map<String, Object> version = (Map<String, Object>) indexStats.get("version");
        assertEquals(2, version.size());
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
    private static void assertNodeStatsMonitoringDoc(final Map<String, Object> document, final TimeValue interval) throws Exception {
        assertMonitoringDoc(document, MonitoredSystem.ES, NodeStatsMonitoringDoc.TYPE, interval);

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

            assertThat(filter + " must not be null in the monitoring document", extractValue(filter, source), notNullValue());
        });
    }

    /**
     * Assert that a {@link ShardMonitoringDoc} contains the expected information
     */
    @SuppressWarnings("unchecked")
    private static void assertShardMonitoringDoc(final Map<String, Object> document, final TimeValue interval) throws Exception {
        assertMonitoringDoc(document, MonitoredSystem.ES, ShardMonitoringDoc.TYPE, interval);

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
            enableMonitoring(collectionInterval);
            runnable.run();
        } finally {
            disableMonitoring();
        }
    }

    /**
     * Enable the monitoring service and the Local exporter, waiting for some monitoring documents
     * to be indexed before it returns.
     */
    public static void enableMonitoring(final TimeValue interval) throws Exception {
        final Map<String, Object> exporters = callRestApi("GET", "/_xpack/usage?filter_path=monitoring.enabled_exporters", 200);
        assertNotNull("List of monitoring exporters must not be null", exporters);
        assertThat("List of enabled exporters must be empty before enabling monitoring",
                    XContentMapValues.extractRawValues("monitoring.enabled_exporters", exporters), hasSize(0));

        final Settings settings = Settings.builder()
                .put("transient.xpack.monitoring.collection.interval", interval.getStringRep())
                .put("transient.xpack.monitoring.exporters._local.enabled", true)
                .build();

        final HttpEntity entity =
                new StringEntity(toXContent(settings, XContentType.JSON, false).utf8ToString(), ContentType.APPLICATION_JSON);

        awaitRestApi("PUT", "/_cluster/settings", emptyMap(), entity,
                response -> {
                    Boolean acknowledged = (Boolean) response.get("acknowledged");
                    return acknowledged != null && acknowledged;
                },"Exception when enabling monitoring");

        awaitRestApi("HEAD", "/.monitoring-es-*", singletonMap("allow_no_indices", "false"), null,
                response -> true,
                "Exception when waiting for monitoring-es-* index to be created");

        awaitRestApi("GET", "/.monitoring-es-*/_search", emptyMap(), null,
                response -> {
                    Number hitsTotal = (Number) XContentMapValues.extractRawValues("hits.total", response).get(0);
                    return hitsTotal != null && hitsTotal.intValue() > 0;
                },"Exception when waiting for monitoring documents to be indexed");
    }

    /**
     * Disable the monitoring service and the Local exporter, waiting for the monitoring indices to
     * be deleted before it returns.
     */
    @SuppressWarnings("unchecked")
    public static void disableMonitoring() throws Exception {
        final Settings settings = Settings.builder()
                .put("transient.xpack.monitoring.collection.interval", (String) null)
                .put("transient.xpack.monitoring.exporters._local.enabled", (String) null)
                .build();

        final HttpEntity entity =
                new StringEntity(toXContent(settings, XContentType.JSON, false).utf8ToString(), ContentType.APPLICATION_JSON);

        awaitRestApi("PUT", "/_cluster/settings", emptyMap(), entity,
                response -> {
                    Boolean acknowledged = (Boolean) response.get("acknowledged");
                    return acknowledged != null && acknowledged;
                },"Exception when disabling monitoring");

        awaitBusy(() -> {
            try {
                Map<String, Object> response = callRestApi("GET", "/_xpack/usage?filter_path=monitoring.enabled_exporters", 200);
                final List<?> exporters = XContentMapValues.extractRawValues("monitoring.enabled_exporters", response);

                if (exporters.isEmpty() == false) {
                    return false;
                }
                response = callRestApi("GET", "/_nodes/_local/stats/thread_pool?filter_path=nodes.*.thread_pool.bulk.active", 200);

                final Map<String, Object> nodes = (Map<String, Object>) response.get("nodes");
                final Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();

                final Number activeBulks = (Number) extractValue("thread_pool.bulk.active", node);
                return activeBulks != null && activeBulks.longValue() == 0L;
            } catch (Exception e) {
                throw new ElasticsearchException("Failed to wait for monitoring exporters to stop:", e);
            }
        });
    }

    /**
     * Executes a request using {@link org.elasticsearch.client.RestClient}, waiting for it to succeed.
     */
    private static void awaitRestApi(final String method, final String endpoint, final Map<String, String> params, final HttpEntity entity,
                              final CheckedFunction<Map<String, Object>, Boolean, IOException> success,
                              final String error) throws Exception {

        final AtomicReference<IOException> exceptionHolder = new AtomicReference<>();
        awaitBusy(() -> {
            try {
                final Response response = client().performRequest(method, endpoint, params, entity);
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    exceptionHolder.set(null);
                    final Map<String, Object> map = ("HEAD".equals(method) == false) ? toMap(response.getEntity()) : null;
                    return success.apply(map);
                }
            } catch (IOException e) {
                exceptionHolder.set(e);
            }
            return false;
        });

        IOException exception = exceptionHolder.get();
        if (exception != null) {
            throw new IllegalStateException(error, exception);
        }
    }

    /**
     * Executes a request using {@link org.elasticsearch.client.RestClient} in a synchronous manner, asserts that the response code
     * is equal to {@code expectedCode} and then returns the {@link Response} as a {@link Map}.
     */
    private static Map<String, Object> callRestApi(final String method, final String endpoint, final int expectedCode) throws Exception {
        final Response response = client().performRequest(method, endpoint);
        assertEquals("Unexpected HTTP response code", expectedCode, response.getStatusLine().getStatusCode());

        return toMap(response.getEntity());
    }

    /**
     * Returns the {@link HttpEntity} content as a {@link Map} object.
     */
    private static Map<String, Object> toMap(final HttpEntity httpEntity) throws IOException {
        final String contentType = httpEntity.getContentType().getValue();
        final XContentType xContentType = XContentType.fromMediaTypeOrFormat(contentType);
        if (xContentType == null) {
            throw new IOException("Content-type not supported [" + contentType + "]");
        }
        return XContentHelper.convertToMap(xContentType.xContent(), httpEntity.getContent(), false);
    }

    /**
     * Execute a Monitoring Bulk request and checks that it returns a 400 error with a given message.
     *
     * @param parameters the request parameters
     * @param httpEntity the request body (can be null)
     * @param matcher a {@link Matcher} to match the message against
     */
    private static void assertBadRequest(final Map<String, String> parameters, final HttpEntity httpEntity, final Matcher<String> matcher) {
        ResponseException responseException = expectThrows(ResponseException.class, () ->
                client().performRequest("POST", "/_xpack/monitoring/_bulk", parameters, httpEntity));

        assertThat(responseException.getMessage(), matcher);
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
    }

    /**
     * Builds a map of parameters for the Monitoring Bulk API
     */
    private static Map<String, String> parameters(final String systemId, final String systemApiVersion, final String interval) {
        final Map<String, String> parameters = new HashMap<>();
        if (systemId != null) {
            parameters.put(RestMonitoringBulkAction.MONITORING_ID, systemId);
        }
        if (systemApiVersion != null) {
            parameters.put(RestMonitoringBulkAction.MONITORING_VERSION, systemApiVersion);
        }
        if (interval != null) {
            parameters.put(RestMonitoringBulkAction.INTERVAL, interval);
        }
        return parameters;
    }

    /**
     * Returns a {@link String} representing a {@link MonitoredSystem} supported by the Monitoring Bulk API
     */
    private static String randomSystemId() {
        return randomSystem().getSystem();
    }

    /**
     * Returns a {@link MonitoredSystem} supported by the Monitoring Bulk API
     */
    private static MonitoredSystem randomSystem() {
        return randomFrom(MonitoredSystem.LOGSTASH, MonitoredSystem.KIBANA, MonitoredSystem.BEATS);
    }
}
