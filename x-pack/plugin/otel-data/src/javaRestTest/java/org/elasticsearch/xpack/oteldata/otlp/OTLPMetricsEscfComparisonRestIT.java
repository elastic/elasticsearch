/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration test that sends identical OTLP metric payloads to two data streams: one using the traditional
 * doc-mode (XContent) indexing path and one using the ESCF (columnar batch) path. The two streams are
 * distinguished solely by the per-index {@code index.time_series.batch_indexing} setting baked into their
 * respective index templates; the cluster-level {@code indices.batch_indexing} setting is enabled at startup.
 * Verifies that both streams produce byte-for-byte equivalent documents when sorted by {@code @timestamp} and
 * {@code _tsid}.
 *
 * <p>Specifically this test guards the contract that
 * {@link org.elasticsearch.cluster.routing.ColumnarTsidCalculator} (used on the ESCF path) and the
 * per-document tsid funnels in {@code otlp/tsid/} (used on the doc path) assign the same {@code _tsid}
 * to every time series, because in TSDB the document {@code _id} is derived from {@code _tsid + @timestamp}.
 *
 * <p>Per-shard doc-count equality across the two three-shard streams further validates that both paths
 * route documents to the same shards.
 */
public class OTLPMetricsEscfComparisonRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    // Dataset base names passed as the data_stream.dataset resource attribute.
    // TargetIndex.sanitizeDataset appends ".otel" automatically, so "docmode" → "docmode.otel"
    // and the resulting data-stream names are metrics-docmode.otel-default / metrics-escf.otel-default.
    private static final String DOCMODE_DATASET = "docmode";
    private static final String ESCF_DATASET = "escf";

    // Workload dimensions
    private static final int NUM_RESOURCES = 5;
    private static final int NUM_DP_ATTR_SETS = 5;
    private static final int NUM_TIMESTAMPS = 100;
    // 5 gauges + 5 monotonic counters per data-point group.
    // Gauges (temporality=null) and cumulative counters (temporality=cumulative) end up in separate
    // document groups because DataPointGroupingContext groups by temporality as part of the grouping key.
    // That produces 2 documents per (resource, dp-attr-set, timestamp) triple.
    private static final int NUM_GAUGES = 5;
    private static final int NUM_COUNTERS = 5;
    private static final int GROUPS_PER_SERIES = 2; // one gauge group + one counter group
    // Expected doc count per stream = NUM_RESOURCES * NUM_DP_ATTR_SETS * NUM_TIMESTAMPS * GROUPS_PER_SERIES
    private static final int EXPECTED_DOCS = NUM_RESOURCES * NUM_DP_ATTR_SETS * NUM_TIMESTAMPS * GROUPS_PER_SERIES;

    private static final InstrumentationScopeInfo SCOPE = InstrumentationScopeInfo.create("io.opentelemetry.escf.comparison.test");

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .feature(FeatureFlag.BATCH_INDEXING)
        .feature(FeatureFlag.INDEX_DIMENSIONS_TSID_OPTIMIZATION_FEATURE_FLAG)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("indices.batch_indexing", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private OtlpHttpMetricExporter exporter;

    @Before
    public void setup() throws Exception {
        // Wait for OTel managed templates to be installed by the plugin.
        assertBusy(() -> assertOK(client().performRequest(new Request("GET", "_index_template/metrics-otel@template"))));

        // Install a doc-mode template at priority 200 (above metrics-otel@template's 120).
        // Uses the same component templates as metrics-otel@template so mappings are identical.
        installTemplate("metrics-docmode.otel-template", "metrics-docmode.otel-*", false);

        // Install the ESCF template — same as doc-mode but with index.time_series.batch_indexing: true
        // so that backing indices created from this template are eligible for the ESCF columnar path.
        installTemplate("metrics-escf.otel-template", "metrics-escf.otel-*", true);

        exporter = OtlpHttpMetricExporter.builder()
            .setEndpoint(getClusterHosts().getFirst().toURI() + "/_otlp/v1/metrics")
            .addHeader("Authorization", "ApiKey " + createApiKey("metrics-docmode.otel-*", "metrics-escf.otel-*"))
            .build();
    }

    @After
    public void teardown() throws Exception {
        if (exporter != null) {
            exporter.shutdown();
        }
        try {
            client().performRequest(new Request("DELETE", "_index_template/metrics-docmode.otel-template"));
        } catch (Exception ignored) {}
        try {
            client().performRequest(new Request("DELETE", "_index_template/metrics-escf.otel-template"));
        } catch (Exception ignored) {}
    }

    /**
     * Sends identical OTLP payloads (differentiated only by {@code data_stream.dataset} resource attribute)
     * to a doc-mode stream and an ESCF-enabled stream, then verifies:
     * <ol>
     *   <li>Both streams contain exactly {@link #EXPECTED_DOCS} documents.</li>
     *   <li>Sorted by {@code @timestamp} + {@code _tsid}, every document in the doc-mode stream has the
     *       same {@code _id}, {@code _tsid}, and {@code _source} as the corresponding document in the ESCF
     *       stream.</li>
     *   <li>Per-primary-shard doc counts are equal across the two three-shard streams.</li>
     * </ol>
     */
    public void testEscfProducesSameDocumentsAsDocMode() throws Exception {
        // Timestamps: NUM_TIMESTAMPS points, 60 seconds apart, ending ~2 minutes before now.
        // All timestamps are within the default 2-hour look_back_time window.
        long nowNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        long endNanos = nowNanos - TimeUnit.MINUTES.toNanos(2);
        long[] timestamps = new long[NUM_TIMESTAMPS];
        for (int t = 0; t < NUM_TIMESTAMPS; t++) {
            // timestamps[0] is oldest (≈ now - 100 minutes), timestamps[99] is newest (≈ now - 2 minutes)
            timestamps[t] = endNanos - TimeUnit.SECONDS.toNanos(60L * (NUM_TIMESTAMPS - 1 - t));
        }

        // Populate the doc-mode stream. The doc-mode template has no index.time_series.batch_indexing
        // setting, so all exports take the XContent path even though indices.batch_indexing is enabled
        // cluster-wide.
        for (int r = 0; r < NUM_RESOURCES; r++) {
            exportSync(buildResourceBatch(DOCMODE_DATASET, r, timestamps));
        }

        // Warm up the ESCF stream: the write index does not yet exist on the first export, so
        // resolveEscfEligible falls back to doc-mode (creating the backing index). Once the
        // backing index exists with index.time_series.batch_indexing: true, subsequent exports
        // take the ESCF columnar path.
        exportSync(buildResourceBatch(ESCF_DATASET, 0, timestamps));
        assertBusy(() -> {
            ObjectPath count = ObjectPath.createFromResponse(
                client().performRequest(new Request("GET", "metrics-escf.otel-default/_count"))
            );
            assertThat((int) count.evaluate("count"), greaterThanOrEqualTo(1));
        });
        for (int r = 1; r < NUM_RESOURCES; r++) {
            exportSync(buildResourceBatch(ESCF_DATASET, r, timestamps));
        }
        refreshAll();

        // -----------------------------------------------------------------------
        // Assertion 1: document counts
        // -----------------------------------------------------------------------
        assertDocCount("metrics-docmode.otel-default", EXPECTED_DOCS);
        assertDocCount("metrics-escf.otel-default", EXPECTED_DOCS);

        // -----------------------------------------------------------------------
        // Assertion 2: sorted document comparison
        // Sort by @timestamp ASC, _tsid ASC and compare every document pairwise.
        // Equal _id values are the strongest signal: TSDB _id = hash(_tsid + @timestamp),
        // so identical _ids prove ColumnarTsidCalculator and the per-document tsid funnels agree.
        // -----------------------------------------------------------------------
        List<Map<String, Object>> docmodeDocs = fetchAllSorted("metrics-docmode.otel-default");
        List<Map<String, Object>> escfDocs = fetchAllSorted("metrics-escf.otel-default");

        assertThat("doc count must match", docmodeDocs.size(), equalTo(escfDocs.size()));
        assertThat("doc count must equal EXPECTED_DOCS", docmodeDocs.size(), equalTo(EXPECTED_DOCS));

        for (int i = 0; i < docmodeDocs.size(); i++) {
            Map<String, Object> docmodeDoc = docmodeDocs.get(i);
            Map<String, Object> escfDoc = escfDocs.get(i);
            String rank = "rank " + i;
            // _id = hash(_tsid + @timestamp): equal _ids prove that ColumnarTsidCalculator
            // and the per-document tsid funnels produce identical _tsid values.
            assertThat(rank + ": _id must match", escfDoc.get("_id"), equalTo(docmodeDoc.get("_id")));
            assertThat(rank + ": _tsid must match", escfDoc.get("_tsid"), equalTo(docmodeDoc.get("_tsid")));
            // Compare _source after stripping data_stream.dataset, which is a constant_keyword whose
            // value is the data-stream's own dataset name and therefore differs between the two streams
            // by construction. Everything else must be byte-for-byte equal.
            @SuppressWarnings("unchecked")
            Map<String, Object> docmodeSource = stripDataStreamDataset((Map<String, Object>) docmodeDoc.get("_source"));
            @SuppressWarnings("unchecked")
            Map<String, Object> escfSource = stripDataStreamDataset((Map<String, Object>) escfDoc.get("_source"));
            assertThat(rank + ": _source (minus data_stream.dataset) must match", escfSource, equalTo(docmodeSource));
        }

        // -----------------------------------------------------------------------
        // Assertion 3: per-shard doc-count equality
        // If _tsid is computed identically then routing (based on _tsid) assigns the
        // same documents to the same shards in both data streams.
        // -----------------------------------------------------------------------
        List<Integer> docmodeShardCounts = primaryShardDocCounts("metrics-docmode.otel-default");
        List<Integer> escfShardCounts = primaryShardDocCounts("metrics-escf.otel-default");
        assertThat("number of primary shards must be equal", escfShardCounts.size(), equalTo(docmodeShardCounts.size()));
        for (int s = 0; s < docmodeShardCounts.size(); s++) {
            assertThat("shard " + s + " doc count must match", escfShardCounts.get(s), equalTo(docmodeShardCounts.get(s)));
        }
    }

    // -------------------------------------------------------------------------
    // Workload builders
    // -------------------------------------------------------------------------

    /**
     * Builds all {@link MetricData} records for one resource (identified by {@code resourceIdx}).
     * Each MetricData record contains {@link #NUM_DP_ATTR_SETS} × {@link #NUM_TIMESTAMPS} data points.
     * The {@code data_stream.dataset} resource attribute controls which data stream the export targets.
     */
    private static List<MetricData> buildResourceBatch(String dataset, int resourceIdx, long[] timestamps) {
        Resource resource = Resource.create(
            Attributes.builder()
                .put(stringKey("service.name"), "elasticsearch")
                .put(stringKey("data_stream.dataset"), dataset)
                .put(stringKey("host.name"), "host-" + resourceIdx)
                .put(stringKey("service.version"), "v" + resourceIdx)
                .build()
        );

        // Pre-build the per-dp-attr-set Attributes objects.
        // attributes.http.method and attributes.http.status_code land under the top-level attributes
        // passthrough object, which also carries time_series_dimension: true.
        Attributes[] dpAttrSets = new Attributes[NUM_DP_ATTR_SETS];
        for (int a = 0; a < NUM_DP_ATTR_SETS; a++) {
            dpAttrSets[a] = Attributes.builder()
                .put(stringKey("http.method"), "METHOD-" + a)
                .put(stringKey("http.status_code"), String.valueOf(200 + a))
                .build();
        }

        List<MetricData> batch = new ArrayList<>(NUM_GAUGES + NUM_COUNTERS);

        // Gauges (double)
        for (int g = 0; g < NUM_GAUGES; g++) {
            List<DoublePointData> points = new ArrayList<>(NUM_DP_ATTR_SETS * NUM_TIMESTAMPS);
            for (int a = 0; a < NUM_DP_ATTR_SETS; a++) {
                for (int t = 0; t < NUM_TIMESTAMPS; t++) {
                    // Unique but deterministic value per (resource, dp-attrs, timestamp)
                    double value = resourceIdx * 10_000.0 + a * 100.0 + t + g * 0.001;
                    points.add(ImmutableDoublePointData.create(timestamps[t], timestamps[t], dpAttrSets[a], value));
                }
            }
            batch.add(
                ImmutableMetricData.createDoubleGauge(
                    resource,
                    SCOPE,
                    "gauge." + g,
                    "Test gauge " + g,
                    "1",
                    ImmutableGaugeData.create(points)
                )
            );
        }

        // Counters (long, monotonic, cumulative)
        for (int c = 0; c < NUM_COUNTERS; c++) {
            List<LongPointData> points = new ArrayList<>(NUM_DP_ATTR_SETS * NUM_TIMESTAMPS);
            for (int a = 0; a < NUM_DP_ATTR_SETS; a++) {
                for (int t = 0; t < NUM_TIMESTAMPS; t++) {
                    long value = (long) resourceIdx * 10_000L + a * 100L + t + c;
                    points.add(ImmutableLongPointData.create(timestamps[t], timestamps[t], dpAttrSets[a], value));
                }
            }
            batch.add(
                ImmutableMetricData.createLongSum(
                    resource,
                    SCOPE,
                    "counter." + c,
                    "Test counter " + c,
                    "1",
                    ImmutableSumData.create(true, AggregationTemporality.CUMULATIVE, points)
                )
            );
        }

        return batch;
    }

    // -------------------------------------------------------------------------
    // Export helpers
    // -------------------------------------------------------------------------

    private void exportSync(List<MetricData> metrics) {
        CompletableResultCode result = exporter.export(metrics).join(30, TimeUnit.SECONDS);
        Throwable failure = result.getFailureThrowable();
        if (failure instanceof Exception e) {
            throw new RuntimeException("OTLP export failed", e);
        } else if (failure != null) {
            throw new RuntimeException("OTLP export failed", failure);
        }
        assertThat("OTLP export must succeed", result.isSuccess(), equalTo(true));
    }

    private static void refreshAll() throws IOException {
        assertOK(client().performRequest(new Request("POST", "metrics-*.otel-default/_refresh")));
    }

    // -------------------------------------------------------------------------
    // Index-template installation
    // -------------------------------------------------------------------------

    /**
     * Installs an index template that overrides {@code metrics-otel@template} (priority 120) for the given
     * pattern at priority 200. Uses the same {@code composed_of} list as the managed OTel template so field
     * mappings are identical.
     *
     * @param batchIndexing when {@code true}, adds {@code index.time_series.batch_indexing: true} to the
     *                      template settings so that backing indices opt in to the ESCF columnar path.
     */
    private static void installTemplate(String name, String pattern, boolean batchIndexing) throws IOException {
        // Disable ILM to prevent lazy rollover during the test window (which would switch the
        // write index and cause past timestamps to be rejected as out-of-range for the new index).
        String batchIndexingSetting = batchIndexing ? """
            ,
                  "index.time_series.batch_indexing": true""" : "";
        Request request = new Request("PUT", "_index_template/" + name);
        request.setJsonEntity("""
            {
              "index_patterns": ["$PATTERN"],
              "priority": 200,
              "data_stream": {},
              "composed_of": [
                "metrics@tsdb-settings",
                "otel@mappings",
                "otel@settings",
                "metrics-otel@mappings",
                "semconv-resource-to-ecs@mappings",
                "metrics@custom",
                "metrics-otel@custom",
                "ecs-tsdb@mappings"
              ],
              "ignore_missing_component_templates": ["metrics@custom", "metrics-otel@custom"],
              "template": {
                "settings": {
                  "index.mode": "time_series",
                  "index.number_of_shards": 3,
                  "index.lifecycle.name": null$BATCH_INDEXING_SETTING
                },
                "mappings": {
                  "properties": {
                    "data_stream.type": {
                      "type": "constant_keyword",
                      "value": "metrics"
                    }
                  }
                }
              }
            }
            """.replace("$PATTERN", pattern).replace("$BATCH_INDEXING_SETTING", batchIndexingSetting));
        assertOK(client().performRequest(request));
    }

    // -------------------------------------------------------------------------
    // Assertion helpers
    // -------------------------------------------------------------------------

    private static void assertDocCount(String target, int expected) throws IOException {
        ObjectPath response = ObjectPath.createFromResponse(client().performRequest(new Request("GET", target + "/_count")));
        assertThat("doc count for " + target, (int) response.evaluate("count"), equalTo(expected));
    }

    /**
     * Returns a copy of {@code source} with the {@code data_stream.dataset} leaf removed.
     * {@code data_stream.dataset} is a constant_keyword whose value is the data-stream's own dataset name,
     * which is intentionally different between the two streams under comparison. All other source fields
     * must be equal.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> stripDataStreamDataset(Map<String, Object> source) {
        Map<String, Object> result = new java.util.LinkedHashMap<>(source);
        Object dsRaw = result.get("data_stream");
        if (dsRaw instanceof Map) {
            Map<String, Object> ds = new java.util.LinkedHashMap<>((Map<String, Object>) dsRaw);
            ds.remove("dataset");
            result.put("data_stream", ds);
        }
        return result;
    }

    /**
     * Fetches all documents from {@code target} sorted by {@code @timestamp} ASC then {@code _tsid} ASC.
     * Each element in the returned list is a map with keys {@code _id}, {@code _tsid}, and {@code _source}.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> fetchAllSorted(String target) throws IOException {
        Request request = new Request("GET", target + "/_search");
        request.setJsonEntity("""
            {
              "size": 10000,
              "sort": [{"@timestamp": "asc"}, {"_tsid": "asc"}],
              "docvalue_fields": ["_tsid"],
              "_source": true,
              "track_total_hits": true
            }
            """);
        ObjectPath response = ObjectPath.createFromResponse(client().performRequest(request));
        List<Object> rawHits = response.evaluate("hits.hits");
        List<Map<String, Object>> result = new ArrayList<>(rawHits.size());
        for (Object rawHit : rawHits) {
            Map<String, Object> hit = (Map<String, Object>) rawHit;
            String id = (String) hit.get("_id");
            // _tsid is returned as a docvalue_fields entry (a list with one element)
            List<Object> tsidList = (List<Object>) ((Map<String, Object>) hit.get("fields")).get("_tsid");
            Object tsid = tsidList != null && tsidList.isEmpty() == false ? tsidList.get(0) : null;
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            result.add(Map.of("_id", id, "_tsid", tsid, "_source", source));
        }
        return result;
    }

    /**
     * Returns the {@code docs.count} value for every primary shard of {@code target}, sorted by shard index.
     * Uses {@code _stats?level=shards} to access per-shard statistics.
     */
    @SuppressWarnings("unchecked")
    private static List<Integer> primaryShardDocCounts(String target) throws IOException {
        ObjectPath stats = ObjectPath.createFromResponse(client().performRequest(new Request("GET", target + "/_stats?level=shards")));
        // Navigate to the write index under _all.primaries — but we need per-shard, so use indices.<name>.shards
        Map<String, Object> indicesMap = stats.evaluate("indices");
        // There is exactly one write index backing this freshly created data stream.
        String indexName = indicesMap.keySet().iterator().next();
        Map<String, Object> shardsMap = (Map<String, Object>) ((Map<String, Object>) indicesMap.get(indexName)).get("shards");
        List<Integer> counts = new ArrayList<>(shardsMap.size());
        // Iterate shards in numeric order (0, 1, 2, ...)
        int numShards = shardsMap.size();
        for (int s = 0; s < numShards; s++) {
            List<Object> shardEntries = (List<Object>) shardsMap.get(String.valueOf(s));
            // Each shard has a primary and optionally replicas; we want the primary.
            for (Object entry : shardEntries) {
                Map<String, Object> shardStat = (Map<String, Object>) entry;
                Boolean routing_primary = (Boolean) ((Map<String, Object>) shardStat.get("routing")).get("primary");
                if (Boolean.TRUE.equals(routing_primary)) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> docs = (Map<String, Object>) shardStat.get("docs");
                    counts.add(((Number) docs.get("count")).intValue());
                    break;
                }
            }
        }
        return counts;
    }

    // -------------------------------------------------------------------------
    // API-key helper (mirrors AbstractOTLPIndexingRestIT)
    // -------------------------------------------------------------------------

    private static String createApiKey(String... indexPatterns) throws IOException {
        StringBuilder indexPatternsJson = new StringBuilder();
        for (int i = 0; i < indexPatterns.length; i++) {
            if (i > 0) {
                indexPatternsJson.append(", ");
            }
            indexPatternsJson.append('"').append(indexPatterns[i]).append('"');
        }
        Request createApiKeyRequest = new Request("POST", "/_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
              "name": "otel-escf-test-key",
              "role_descriptors": {
                "writer": {
                  "index": [
                    {
                      "names": [$INDEX_PATTERNS],
                      "privileges": ["create_doc", "auto_configure"]
                    }
                  ]
                }
              }
            }
            """.replace("$INDEX_PATTERNS", indexPatternsJson.toString()));
        ObjectPath createApiKeyResponse = ObjectPath.createFromResponse(client().performRequest(createApiKeyRequest));
        return createApiKeyResponse.evaluate("encoded");
    }
}
