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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.equalTo;

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

    private static final Logger logger = LogManager.getLogger(OTLPMetricsEscfComparisonRestIT.class);

    // Dataset base names passed as the data_stream.dataset resource attribute.
    // TargetIndex.sanitizeDataset appends ".otel" automatically, so "docmode" → "docmode.otel"
    // and the resulting data-stream names are metrics-docmode.otel-default / metrics-escf.otel-default.
    private static final String DOCMODE_DATASET = "docmode";
    private static final String ESCF_DATASET = "escf";

    private static final int NUM_RESOURCES = 3;
    private static final int NUM_DP_ATTR_SETS = 3;
    // numGauges, numCounters, and numTimestamps are randomised per test run.

    private static final InstrumentationScopeInfo SCOPE = InstrumentationScopeInfo.create("io.opentelemetry.escf.comparison.test");

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .feature(FeatureFlag.BATCH_INDEXING)
        .feature(FeatureFlag.INDEX_DIMENSIONS_TSID_OPTIMIZATION_FEATURE_FLAG)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("indices.batch_indexing", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    // Shared across all iterations: created on the first @Before, shut down in @AfterClass.
    // Templates are also installed once and deleted in @AfterClass.
    private static OtlpHttpMetricExporter exporter;
    private static volatile boolean classSetupDone = false;

    /**
     * Skip ESRestTestCase's between-iteration cluster wipe (wipeCluster, resetFeatureStates, etc.).
     * We manage our own cleanup — data streams are deleted in {@link #teardown()}, templates and the
     * exporter are torn down in {@link #teardownClass()}.
     */
    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        classSetupDone = false;
        if (exporter != null) {
            exporter.shutdown();
            exporter = null;
        }
        try {
            client().performRequest(new Request("DELETE", "_index_template/metrics-docmode.otel-template"));
        } catch (Exception ignored) {}
        try {
            client().performRequest(new Request("DELETE", "_index_template/metrics-escf.otel-template"));
        } catch (Exception ignored) {}
        deleteDataStreams();
    }

    @Before
    public void setup() throws Exception {
        // Drop leftover data streams from a previous iteration (no-op on first run).
        // This prevents segments from accumulating across iterations
        deleteDataStreams();

        if (classSetupDone == false) {
            // Wait for OTel managed templates to be installed by the plugin.
            assertBusy(() -> assertOK(client().performRequest(new Request("GET", "_index_template/metrics-otel@template"))));

            // Install a doc-mode template at priority 200 (above metrics-otel@template's 120).
            installTemplate("metrics-docmode.otel-template", "metrics-docmode.otel-*", false);

            // Install the ESCF template — same but with index.time_series.batch_indexing: true.
            installTemplate("metrics-escf.otel-template", "metrics-escf.otel-*", true);

            // Build the exporter once. Security is disabled so no auth header is needed.
            String firstHost = cluster.getHttpAddresses().split(",")[0].trim();
            exporter = OtlpHttpMetricExporter.builder().setEndpoint("http://" + firstHost + "/_otlp/v1/metrics").build();

            classSetupDone = true;
        }
    }

    @After
    public void teardown() {
        deleteDataStreams();
    }

    private static void deleteDataStreams() {
        for (String ds : List.of("metrics-docmode.otel-default", "metrics-escf.otel-default")) {
            try {
                client().performRequest(new Request("DELETE", "_data_stream/" + ds));
                logger.info("Deleted data stream [{}]", ds);
            } catch (Exception e) {
                logger.debug("Could not delete data stream [{}] (may not exist yet): {}", ds, e.getMessage());
            }
        }
    }

    /**
     * Sends identical OTLP payloads (differentiated only by {@code data_stream.dataset} resource attribute)
     * to a doc-mode stream and an ESCF-enabled stream, then verifies:
     * <ol>
     *   <li>Both streams contain exactly {@code expectedDocs} documents (randomised per run).</li>
     *   <li>Sorted by {@code @timestamp} + {@code _tsid}, every document in the doc-mode stream has the
     *       same {@code _id}, {@code _tsid}, and {@code _source} as the corresponding document in the ESCF
     *       stream.</li>
     *   <li>Per-primary-shard doc counts are equal across the two three-shard streams.</li>
     * </ol>
     */
    public void testEscfProducesSameDocumentsAsDocMode() throws Exception {
        int numGauges = randomIntBetween(1, 3);
        int numCounters = randomIntBetween(1, 3);
        int numTimestamps = randomIntBetween(100, 1000);
        // Each OTLP message (MetricData) covers 1–5 consecutive timestamps: NUM_DP_ATTR_SETS × numTimestampsPerMsg
        // data points per metric, varying per run. This tests that the ingest pipeline correctly
        // separates multi-timestamp messages into one TSDB document per (timestamp, attr-set).
        int numTimestampsPerMsg = randomIntBetween(1, 5);
        // Each timestamp batch is split into 1–3 separate OTLP exports (each covering a disjoint
        // subset of metric names for all resources at all timestamps in the batch).
        int numSplits = randomIntBetween(1, 3);
        int expectedDocs = NUM_RESOURCES * (numGauges + numCounters) * NUM_DP_ATTR_SETS * numTimestamps;

        // Timestamps: numTimestamps points, 5 seconds apart, ending ~2 minutes before now.
        // 1000 × 5 s = 5000 s ≈ 83 minutes, safely within the default 2-hour look_back_time window.
        long nowNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        long endNanos = nowNanos - TimeUnit.MINUTES.toNanos(2);
        long[] timestamps = new long[numTimestamps];
        for (int t = 0; t < numTimestamps; t++) {
            timestamps[t] = endNanos - TimeUnit.SECONDS.toNanos(5L * (numTimestamps - 1 - t));
        }

        // Timestamps are grouped into export batches (EXPORT_BATCH timestamps) to bound each HTTP payload.
        // Within each batch, timestamps are further grouped into messages of numTimestampsPerMsg.
        // Max index ops per export: EXPORT_BATCH × NUM_RESOURCES × (numGauges + numCounters) × NUM_DP_ATTR_SETS
        // = 10 × 3 × 6 × 3 = 540 (EXPORT_BATCH is in timestamps, so numTimestampsPerMsg does not affect this).
        final int EXPORT_BATCH = 10;
        // Periodic refresh (every REFRESH_EVERY batches) clears the Lucene LiveVersionMap so that
        // heap usage stays bounded even with refresh_interval=-1.
        final int REFRESH_EVERY = 10;

        // Populate the doc-mode stream. The doc-mode template has no index.time_series.batch_indexing
        // setting, so all exports take the XContent path even though indices.batch_indexing is enabled
        // cluster-wide.
        // Within each timestamp batch, timestamps are grouped into messages of numTimestampsPerMsg.
        // Each message group is then distributed round-robin across numSplits sub-batches to exercise
        // multiple OTLP exports per data stream.
        for (int tStart = 0; tStart < numTimestamps; tStart += EXPORT_BATCH) {
            int tEnd = Math.min(tStart + EXPORT_BATCH, numTimestamps);
            List<List<MetricData>> splits = new ArrayList<>(numSplits);
            for (int s = 0; s < numSplits; s++) {
                splits.add(new ArrayList<>());
            }
            for (int msgStart = tStart; msgStart < tEnd; msgStart += numTimestampsPerMsg) {
                long[] msgTs = Arrays.copyOfRange(timestamps, msgStart, Math.min(msgStart + numTimestampsPerMsg, tEnd));
                for (int r = 0; r < NUM_RESOURCES; r++) {
                    List<MetricData> metrics = buildResourceBatch(DOCMODE_DATASET, r, msgTs, numGauges, numCounters);
                    for (int m = 0; m < metrics.size(); m++) {
                        splits.get(m % numSplits).add(metrics.get(m));
                    }
                }
            }
            for (List<MetricData> split : splits) {
                if (split.isEmpty() == false) {
                    exportSync(split);
                }
            }
            if ((tStart / EXPORT_BATCH + 1) % REFRESH_EVERY == 0) {
                refreshAll();
            }
        }

        // Populate the ESCF stream. The first export naturally falls back to doc-mode (resolveEscfEligible
        // returns false when the data stream does not yet exist) which creates the backing index; all
        // subsequent exports take the ESCF path. No explicit warm-up is needed.
        for (int tStart = 0; tStart < numTimestamps; tStart += EXPORT_BATCH) {
            int tEnd = Math.min(tStart + EXPORT_BATCH, numTimestamps);
            List<List<MetricData>> splits = new ArrayList<>(numSplits);
            for (int s = 0; s < numSplits; s++) {
                splits.add(new ArrayList<>());
            }
            for (int msgStart = tStart; msgStart < tEnd; msgStart += numTimestampsPerMsg) {
                long[] msgTs = Arrays.copyOfRange(timestamps, msgStart, Math.min(msgStart + numTimestampsPerMsg, tEnd));
                for (int r = 0; r < NUM_RESOURCES; r++) {
                    List<MetricData> metrics = buildResourceBatch(ESCF_DATASET, r, msgTs, numGauges, numCounters);
                    for (int m = 0; m < metrics.size(); m++) {
                        splits.get(m % numSplits).add(metrics.get(m));
                    }
                }
            }
            for (List<MetricData> split : splits) {
                if (split.isEmpty() == false) {
                    exportSync(split);
                }
            }
            if ((tStart / EXPORT_BATCH + 1) % REFRESH_EVERY == 0) {
                refreshAll();
            }
        }
        refreshAll();

        // -----------------------------------------------------------------------
        // Assertion 1: document counts
        // -----------------------------------------------------------------------
        assertDocCount("metrics-docmode.otel-default", expectedDocs);
        assertDocCount("metrics-escf.otel-default", expectedDocs);

        // -----------------------------------------------------------------------
        // Assertion 2: per-range document comparison.
        // Timestamps are batched into groups of batchTimestamps. For each batch we issue one range
        // query per stream, capping the response at batchTimestamps * docsPerTimestamp docs — well
        // within the default max_result_window.
        // -----------------------------------------------------------------------
        final int docsPerTimestamp = NUM_RESOURCES * (numGauges + numCounters) * NUM_DP_ATTR_SETS;
        final int batchTimestamps = 10;
        int docsCompared = 0;
        for (int batchStart = 0; batchStart < timestamps.length; batchStart += batchTimestamps) {
            int batchEnd = Math.min(batchStart + batchTimestamps - 1, timestamps.length - 1);
            long startMillis = TimeUnit.NANOSECONDS.toMillis(timestamps[batchStart]);
            long endMillis = TimeUnit.NANOSECONDS.toMillis(timestamps[batchEnd]);
            int expectedBatchDocs = (batchEnd - batchStart + 1) * docsPerTimestamp;
            List<Map<String, Object>> docmodeDocs = fetchSortedForRange("metrics-docmode.otel-default", startMillis, endMillis);
            List<Map<String, Object>> escfDocs = fetchSortedForRange("metrics-escf.otel-default", startMillis, endMillis);
            String rangeCtx = "[" + startMillis + "," + endMillis + "]";
            assertThat("doc count must match for range " + rangeCtx, escfDocs.size(), equalTo(docmodeDocs.size()));
            assertThat("doc count for range " + rangeCtx, docmodeDocs.size(), equalTo(expectedBatchDocs));
            for (int i = 0; i < docmodeDocs.size(); i++) {
                Map<String, Object> docmodeDoc = docmodeDocs.get(i);
                Map<String, Object> escfDoc = escfDocs.get(i);
                String ctx = rangeCtx + " rank=" + i;
                assertThat(ctx + ": _id must match", escfDoc.get("_id"), equalTo(docmodeDoc.get("_id")));
                assertThat(ctx + ": _tsid must match", escfDoc.get("_tsid"), equalTo(docmodeDoc.get("_tsid")));
                @SuppressWarnings("unchecked")
                Map<String, Object> docmodeRawSource = (Map<String, Object>) docmodeDoc.get("_source");
                @SuppressWarnings("unchecked")
                Map<String, Object> escfRawSource = (Map<String, Object>) escfDoc.get("_source");
                assertSourceShape(ctx + " docmode", docmodeRawSource);
                assertSourceShape(ctx + " escf", escfRawSource);
                Map<String, Object> docmodeSource = stripDataStreamDataset(docmodeRawSource);
                Map<String, Object> escfSource = stripDataStreamDataset(escfRawSource);
                assertThat(ctx + ": _source (minus data_stream.dataset) must match", escfSource, equalTo(docmodeSource));
            }
            docsCompared += docmodeDocs.size();
        }
        assertThat("total docs compared must equal expectedDocs", docsCompared, equalTo(expectedDocs));

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
     * Builds all {@link MetricData} records for one resource (identified by {@code resourceIdx})
     * covering the timestamps in {@code msgTimestamps}. Each MetricData contains
     * {@link #NUM_DP_ATTR_SETS} × {@code msgTimestamps.length} data points — one per (attr-set, timestamp)
     * pair. The {@code data_stream.dataset} resource attribute controls which data stream the export targets.
     */
    private static List<MetricData> buildResourceBatch(
        String dataset,
        int resourceIdx,
        long[] msgTimestamps,
        int numGauges,
        int numCounters
    ) {
        Resource resource = Resource.create(
            Attributes.builder()
                .put(stringKey("service.name"), "elasticsearch")
                .put(stringKey("data_stream.dataset"), dataset)
                .put(stringKey("host.name"), "host-" + resourceIdx)
                .put(stringKey("service.version"), "v" + resourceIdx)
                .build()
        );

        List<MetricData> batch = new ArrayList<>(numGauges + numCounters);

        // Gauges (double). Each gauge gets its own set of attribute values (metric index embedded) so
        // every (gauge, attr-set) pair is a distinct time series / TSDB document group.
        for (int g = 0; g < numGauges; g++) {
            List<DoublePointData> points = new ArrayList<>(NUM_DP_ATTR_SETS * msgTimestamps.length);
            for (long timestamp : msgTimestamps) {
                for (int a = 0; a < NUM_DP_ATTR_SETS; a++) {
                    Attributes dpAttrs = Attributes.builder()
                        .put(stringKey("http.method"), "G" + g + "-METHOD-" + a)
                        .put(stringKey("http.status_code"), String.valueOf(200 + g * NUM_DP_ATTR_SETS + a))
                        .build();
                    double value = resourceIdx * 10_000.0 + a * 100.0 + g * 0.001;
                    points.add(ImmutableDoublePointData.create(timestamp, timestamp, dpAttrs, value));
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

        // Counters (long, monotonic, cumulative). Same uniqueness guarantee as gauges.
        for (int c = 0; c < numCounters; c++) {
            List<LongPointData> points = new ArrayList<>(NUM_DP_ATTR_SETS * msgTimestamps.length);
            for (long timestamp : msgTimestamps) {
                for (int a = 0; a < NUM_DP_ATTR_SETS; a++) {
                    Attributes dpAttrs = Attributes.builder()
                        .put(stringKey("http.method"), "C" + c + "-METHOD-" + a)
                        .put(stringKey("http.status_code"), String.valueOf(300 + c * NUM_DP_ATTR_SETS + a))
                        .build();
                    long value = (long) resourceIdx * 10_000L + a * 100L + c;
                    points.add(ImmutableLongPointData.create(timestamp, timestamp, dpAttrs, value));
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
                  "index.number_of_replicas": 0,
                  "index.refresh_interval": "-1",
                  "index.translog.durability": "async",
                  "index.lifecycle.name": null,
                  "index.requests.cache.enable": false$BATCH_INDEXING_SETTING
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

    /** Asserts that {@code source} is a well-formed OTel metric document with top-level {@code attributes}
     * and a {@code resource} object that itself contains {@code attributes}. */
    @SuppressWarnings("unchecked")
    private static void assertSourceShape(String ctx, Map<String, Object> source) {
        assertFalse(ctx + ": _source must not be empty", source.isEmpty());
        assertTrue(ctx + ": _source must contain top-level 'attributes'", source.containsKey("attributes"));
        Object resourceRaw = source.get("resource");
        assertNotNull(ctx + ": _source must contain 'resource'", resourceRaw);
        assertThat(ctx + ": 'resource' must be a map", resourceRaw, org.hamcrest.Matchers.instanceOf(Map.class));
        Map<String, Object> resource = (Map<String, Object>) resourceRaw;
        assertTrue(ctx + ": 'resource' must contain 'attributes'", resource.containsKey("attributes"));
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
     * Fetches all documents from {@code target} whose {@code @timestamp} falls in
     * {@code [startMillis, endMillis]}, sorted by {@code @timestamp} ASC then {@code _tsid} ASC.
     * Each element in the returned list is a map with keys {@code _id}, {@code _tsid}, and {@code _source}.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> fetchSortedForRange(String target, long startMillis, long endMillis) throws IOException {
        Request request = new Request("GET", target + "/_search");
        request.setJsonEntity(String.format(java.util.Locale.ROOT, """
            {
              "size": 10000,
              "query": {"range": {"@timestamp": {"gte": %d, "lte": %d}}},
              "sort": [{"@timestamp": "asc"}, {"_tsid": "asc"}],
              "docvalue_fields": ["_tsid"],
              "_source": true
            }
            """, startMillis, endMillis));
        ObjectPath response = ObjectPath.createFromResponse(client().performRequest(request));
        List<Object> rawHits = response.evaluate("hits.hits");
        List<Map<String, Object>> result = new ArrayList<>(rawHits.size());
        for (Object rawHit : rawHits) {
            Map<String, Object> hit = (Map<String, Object>) rawHit;
            String id = (String) hit.get("_id");
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
}
