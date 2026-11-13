/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.opentelemetry.sdk.metrics.data.AggregationTemporality.CUMULATIVE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration test that sends identical OTLP metric payloads to two data streams: one using the traditional
 * doc-mode (XContent) indexing path and one using the ESCF (columnar batch) path. The two streams are
 * distinguished solely by the per-index {@code index.batch_indexing} setting baked into their
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
 *
 * <p>Attributes are entirely random: which keys exist at each level, which shape from {@link AttributeShape}
 * each key uses, which subset of keys each series carries (possibly none) and their values. The payload is
 * sent in two ways, one per test:
 * <ul>
 *   <li>{@link #testProtobufPayloads()} builds OTLP protobuf directly, which can express every shape.</li>
 *   <li>{@link #testSdkPayloads()} goes end to end through the OpenTelemetry SDK's exporter, as real clients do.
 *       Its {@code Attributes} API can only express {@link #SDK_SHAPES}, but it provides a real end-to-end test.</li>
 * </ul>
 *
 * <p>Because a single unsupported value makes the whole export fall back to doc mode — which would make this
 * comparison pass vacuously — both tests also assert that the ESCF path was genuinely taken; see
 * {@link #countEscfBatches()}.
 */
public class OTLPMetricsEscfComparisonRestIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(OTLPMetricsEscfComparisonRestIT.class);

    // Dataset base names passed as the data_stream.dataset resource attribute.
    // TargetIndex.sanitizeDataset appends ".otel" automatically, so "docmode" → "docmode.otel"
    // and the resulting data-stream names are metrics-docmode.otel-default / metrics-escf.otel-default.
    private static final String DOCMODE_DATASET = "docmode";
    private static final String ESCF_DATASET = "escf";
    private static final String DOCMODE_STREAM = "metrics-docmode.otel-default";
    private static final String ESCF_STREAM = "metrics-escf.otel-default";

    private static final String SCOPE_NAME = "io.opentelemetry.escf.comparison.test";
    private static final String SCOPE_VERSION = "1.0.0";

    /** Logged by {@link org.elasticsearch.xpack.oteldata.otlp.OTLPMetricsTransportAction} once per ESCF export. */
    private static final String ESCF_BATCH_LOG = "built an ESCF batch of";
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
        // The coordinator logs which path each OTLP export took; countEscfBatches reads it back.
        .setting("logger.org.elasticsearch.xpack.oteldata.otlp.OTLPMetricsTransportAction", "TRACE")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    // Shared across all iterations: created on the first @Before, shut down in @AfterClass.
    // Templates are also installed once and deleted in @AfterClass.
    private static OtlpHttpMetricExporter sdkExporter;
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
        if (sdkExporter != null) {
            sdkExporter.shutdown();
            sdkExporter = null;
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

            // Install the ESCF template — same but with index.batch_indexing: true.
            installTemplate("metrics-escf.otel-template", "metrics-escf.otel-*", true);

            // Build the exporter once. Security is disabled so no auth header is needed.
            String firstHost = cluster.getHttpAddresses().split(",")[0].trim();
            sdkExporter = OtlpHttpMetricExporter.builder().setEndpoint("http://" + firstHost + "/_otlp/v1/metrics").build();

            classSetupDone = true;
        }
    }

    @After
    public void teardown() {
        deleteDataStreams();
    }

    private static void deleteDataStreams() {
        for (String ds : List.of(DOCMODE_STREAM, ESCF_STREAM)) {
            try {
                client().performRequest(new Request("DELETE", "_data_stream/" + ds));
                logger.info("Deleted data stream [{}]", ds);
            } catch (Exception e) {
                logger.debug("Could not delete data stream [{}] (may not exist yet): {}", ds, e.getMessage());
            }
        }
    }

    /** Sends payloads built as OTLP protobuf directly, which can carry every {@link AttributeShape}. */
    public void testProtobufPayloads() throws Exception {
        assertEscfMatchesDocMode(randomWorkload(EnumSet.allOf(AttributeShape.class)), OTLPMetricsEscfComparisonRestIT::exportProto);
    }

    /** Sends payloads end to end through the OpenTelemetry SDK's exporter, limited to {@link #SDK_SHAPES}. */
    public void testSdkPayloads() throws Exception {
        assertEscfMatchesDocMode(randomWorkload(SDK_SHAPES), OTLPMetricsEscfComparisonRestIT::exportSdk);
    }

    /**
     * Sends identical OTLP payloads (differentiated only by {@code data_stream.dataset} resource attribute)
     * through {@code sender} to a doc-mode stream and an ESCF-enabled stream, then verifies:
     * <ol>
     *   <li>No data point is rejected, and both streams contain the same number of documents. The count is
     *       not predicted, because metrics that happen to share an attribute set merge into one document.</li>
     *   <li>Sorted by {@code @timestamp} + {@code _tsid}, every document in the doc-mode stream has the
     *       same {@code _id}, {@code _tsid}, and {@code _source} as the corresponding document in the ESCF
     *       stream.</li>
     *   <li>Per-primary-shard doc counts are equal across the two three-shard streams.</li>
     *   <li>Both streams derive the same mappings from their documents.</li>
     *   <li>Every export to the ESCF stream was actually written through the ESCF path.</li>
     * </ol>
     */
    private void assertEscfMatchesDocMode(Workload workload, Sender sender) throws Exception {
        int numTimestamps = randomIntBetween(1, 20);
        // Each OTLP message (MetricData) covers 1–5 consecutive timestamps, which exercises splitting a
        // multi-timestamp message into one TSDB document per (timestamp, attribute set).
        int numTimestampsPerMsg = randomIntBetween(1, 5);
        // The payload is split into 1–3 OTLP exports, each covering a disjoint subset of the metrics.
        int numSplits = randomIntBetween(1, 3);

        // Timestamps 5 seconds apart, ending 2 minutes before now: well within the default look_back_time.
        long endNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()) - TimeUnit.MINUTES.toNanos(2);
        long[] timestamps = new long[numTimestamps];
        for (int t = 0; t < numTimestamps; t++) {
            timestamps[t] = endNanos - TimeUnit.SECONDS.toNanos(5L * (numTimestamps - 1 - t));
        }

        // The ESCF path only applies to an existing data stream, so both are created before the first export.
        createDataStream(DOCMODE_STREAM);
        createDataStream(ESCF_STREAM);
        // The doc-mode template has no index.batch_indexing setting, so its exports take the
        // XContent path even though indices.batch_indexing is enabled cluster-wide.
        populate(sender, DOCMODE_DATASET, workload, timestamps, numTimestampsPerMsg, numSplits);
        int escfBatchesBefore = countEscfBatches();
        int escfExports = populate(sender, ESCF_DATASET, workload, timestamps, numTimestampsPerMsg, numSplits);
        assertThat(
            "every ESCF export must have been written as an ESCF batch",
            countEscfBatches() - escfBatchesBefore,
            equalTo(escfExports)
        );
        refreshAll();

        // At most 20 timestamps × 3 resources × 6 metrics × 3 attribute sets = 1080 documents, so a single
        // search per stream returns everything.
        List<Map<String, Object>> docmodeDocs = fetchSorted(DOCMODE_STREAM);
        List<Map<String, Object>> escfDocs = fetchSorted(ESCF_STREAM);
        assertThat("the doc-mode stream must not be empty", docmodeDocs.size(), greaterThan(0));
        assertThat("doc count must match", escfDocs.size(), equalTo(docmodeDocs.size()));
        for (int i = 0; i < docmodeDocs.size(); i++) {
            Map<String, Object> docmodeDoc = docmodeDocs.get(i);
            Map<String, Object> escfDoc = escfDocs.get(i);
            String ctx = "rank=" + i;
            assertThat(ctx + ": _id must match", escfDoc.get("_id"), equalTo(docmodeDoc.get("_id")));
            assertThat(ctx + ": _tsid must match", escfDoc.get("_tsid"), equalTo(docmodeDoc.get("_tsid")));
            @SuppressWarnings("unchecked")
            Map<String, Object> docmodeSource = stripDataStreamDataset((Map<String, Object>) docmodeDoc.get("_source"));
            @SuppressWarnings("unchecked")
            Map<String, Object> escfSource = stripDataStreamDataset((Map<String, Object>) escfDoc.get("_source"));
            assertThat(ctx + ": _source (minus data_stream.dataset) must match", escfSource, equalTo(docmodeSource));
        }

        // If _tsid is computed identically then routing (based on _tsid) assigns the same documents to the same
        // shards in both data streams.
        assertThat("per-shard doc counts must match", primaryShardDocCounts(ESCF_STREAM), equalTo(primaryShardDocCounts(DOCMODE_STREAM)));

        // The two paths reach the mapper through different code (columnar vs document parsing), so an attribute
        // shape could round-trip to the same _source while producing a different field type.
        assertThat("mappings must match", indexMappings(ESCF_STREAM), equalTo(indexMappings(DOCMODE_STREAM)));
    }

    /**
     * Counts the OTLP exports written as ESCF batches so far. Without checking this, an attribute shape the
     * batch builder rejects would silently fall back to doc mode, and every comparison would pit doc-mode
     * output against doc-mode output and pass for the wrong reason.
     *
     * <p>Reads the node log rather than any stat, because batch indexing exposes no counter: the one signal
     * that distinguishes the two paths is what {@code OTLPMetricsTransportAction} logs about its own
     * decision. The log spans all iterations, so callers compare counts taken before and after.
     */
    private static int countEscfBatches() throws IOException {
        try (InputStream log = cluster.getNodeLog(0, LogType.SERVER)) {
            return (int) Streams.readAllLines(log).stream().filter(line -> line.contains(ESCF_BATCH_LOG)).count();
        }
    }

    /**
     * The attribute value shapes the ESCF path supports. Bytes values, key-value (object) values and arrays
     * nested inside arrays are deliberately
     * absent: each makes the row ineligible, which would send the whole export down the doc-mode path and
     * defeat the check in {@link #countEscfBatches()}.
     *
     * <p>A key keeps its shape for the whole run. Dynamic mapping takes the type of whichever document
     * reaches the shard first, so a key whose value were sometimes a string and sometimes a number could
     * resolve to different types in the two streams purely by timing.
     */
    private enum AttributeShape {
        STRING,
        LONG,
        DOUBLE,
        BOOLEAN,
        STRING_ARRAY,
        LONG_ARRAY,
        DOUBLE_ARRAY,
        BOOLEAN_ARRAY,
        /** An array whose elements are a mix of ints and doubles, always leading with a double. */
        MIXED_NUMERIC_ARRAY,
        /** An array of strings with unset elements interspersed, always leading with a string. */
        ARRAY_WITH_NULLS,
        /** Always empty, so neither path should derive a mapping for the key at all. */
        EMPTY_ARRAY,
        /** The value oneof left unset: dropped by the ESCF path, written as a null by the doc path. */
        UNSET
    }

    /** The shapes the OpenTelemetry SDK's {@code Attributes} API can express: scalars and homogeneous arrays. */
    private static final Set<AttributeShape> SDK_SHAPES = EnumSet.of(
        AttributeShape.STRING,
        AttributeShape.LONG,
        AttributeShape.DOUBLE,
        AttributeShape.BOOLEAN,
        AttributeShape.STRING_ARRAY,
        AttributeShape.LONG_ARRAY,
        AttributeShape.DOUBLE_ARRAY,
        AttributeShape.BOOLEAN_ARRAY
    );

    /** An attribute key together with the single value shape it uses for the whole run. */
    private record AttributeSpec(String key, AttributeShape shape) {}

    /** A metric name together with the point type it uses for the whole run. */
    private record MetricSpec(String name, boolean sum, boolean longValued) {}

    /**
     * The randomised payload, generated once and sent verbatim to both data streams. Attribute values are
     * fixed per time series rather than per data point, so that a series keeps one {@code _tsid} across all
     * timestamps.
     *
     * <p>Resources are distinct from each other, and so are the attribute sets of each metric, so that no two
     * data points of a metric share a series and a timestamp: grouping would reject the second one as a
     * duplicate. Different metrics may share an attribute set, in which case grouping merges them into one
     * document, so the document count is not predictable.
     *
     * @param resourceAttributes  indexed by resource
     * @param dataPointAttributes indexed by metric, then by attribute set
     */
    private record Workload(
        List<MetricSpec> metrics,
        List<List<KeyValue>> resourceAttributes,
        List<KeyValue> scopeAttributes,
        List<List<List<KeyValue>>> dataPointAttributes
    ) {}

    /** A random workload whose attribute keys only use {@code shapes}. */
    private static Workload randomWorkload(Set<AttributeShape> shapes) {
        List<MetricSpec> metrics = new ArrayList<>();
        for (int m = 0, numMetrics = randomIntBetween(1, 6); m < numMetrics; m++) {
            metrics.add(new MetricSpec("metric." + m, randomBoolean(), randomBoolean()));
        }

        List<AttributeShape> shapeList = List.copyOf(shapes);
        List<AttributeSpec> resourceSchema = randomAttributeSchema("test.resource", shapeList);
        List<AttributeSpec> scopeSchema = randomAttributeSchema("test.scope", shapeList);
        List<AttributeSpec> dataPointSchema = randomAttributeSchema("test.datapoint", shapeList);

        List<List<KeyValue>> resourceAttributes = distinctRandomAttributes(resourceSchema, randomIntBetween(1, 3));
        List<List<List<KeyValue>>> dataPointAttributes = new ArrayList<>(metrics.size());
        for (int m = 0; m < metrics.size(); m++) {
            dataPointAttributes.add(distinctRandomAttributes(dataPointSchema, randomIntBetween(1, 3)));
        }
        return new Workload(metrics, resourceAttributes, randomAttributes(scopeSchema), dataPointAttributes);
    }

    /**
     * Up to {@code count} random attribute sets over {@code schema} that are distinct series. Fewer come back
     * when the schema cannot produce that many, e.g. a schema without keys produces exactly one, empty, set.
     */
    private static List<List<KeyValue>> distinctRandomAttributes(List<AttributeSpec> schema, int count) {
        Map<Map<String, AnyValue>, List<KeyValue>> distinct = new LinkedHashMap<>();
        for (int attempt = 0; attempt < count * 10 && distinct.size() < count; attempt++) {
            List<KeyValue> attributes = randomAttributes(schema);
            distinct.putIfAbsent(seriesIdentity(attributes), attributes);
        }
        return new ArrayList<>(distinct.values());
    }

    /**
     * What makes two attribute sets the same series. Attribute order does not count, and neither does
     * anything that contributes nothing to the tsid: an unset value, an unset array element, or an array that
     * is empty once those are removed.
     */
    private static Map<String, AnyValue> seriesIdentity(List<KeyValue> attributes) {
        Map<String, AnyValue> identity = new HashMap<>();
        for (KeyValue kv : attributes) {
            AnyValue value = kv.getValue();
            if (value.hasArrayValue()) {
                List<AnyValue> elements = value.getArrayValue()
                    .getValuesList()
                    .stream()
                    .filter(e -> e.getValueCase() != AnyValue.ValueCase.VALUE_NOT_SET)
                    .toList();
                if (elements.isEmpty() == false) {
                    identity.put(kv.getKey(), arrayValue(elements));
                }
            } else if (value.getValueCase() != AnyValue.ValueCase.VALUE_NOT_SET) {
                identity.put(kv.getKey(), value);
            }
        }
        return identity;
    }

    /**
     * Between zero and eight keys, each with a random shape from {@code shapes}. A key keeps its shape for the
     * whole run, which is what keeps dynamic mapping deterministic; see {@link AttributeShape}.
     */
    private static List<AttributeSpec> randomAttributeSchema(String prefix, List<AttributeShape> shapes) {
        List<AttributeSpec> schema = new ArrayList<>();
        for (int i = 0, numKeys = randomIntBetween(0, 8); i < numKeys; i++) {
            schema.add(new AttributeSpec(prefix + ".attr" + i, randomFrom(shapes)));
        }
        return schema;
    }

    /** A random subset of {@code schema}, possibly empty, in random order and with random values. */
    private static List<KeyValue> randomAttributes(List<AttributeSpec> schema) {
        List<KeyValue> attributes = new ArrayList<>(schema.size());
        for (AttributeSpec spec : schema) {
            if (randomBoolean()) {
                attributes.add(KeyValue.newBuilder().setKey(spec.key()).setValue(randomValue(spec.shape())).build());
            }
        }
        Collections.shuffle(attributes, random());
        return attributes;
    }

    private static AnyValue randomValue(AttributeShape shape) {
        return switch (shape) {
            case STRING -> stringValue(randomAlphaOfLengthBetween(1, 16));
            case LONG -> AnyValue.newBuilder().setIntValue(randomLong()).build();
            case DOUBLE -> AnyValue.newBuilder().setDoubleValue(randomDouble()).build();
            case BOOLEAN -> AnyValue.newBuilder().setBoolValue(randomBoolean()).build();
            case STRING_ARRAY -> arrayValue(randomElements(() -> stringValue(randomAlphaOfLengthBetween(1, 8))));
            case LONG_ARRAY -> arrayValue(randomElements(() -> AnyValue.newBuilder().setIntValue(randomLong()).build()));
            case DOUBLE_ARRAY -> arrayValue(randomElements(() -> AnyValue.newBuilder().setDoubleValue(randomDouble()).build()));
            case BOOLEAN_ARRAY -> arrayValue(randomElements(() -> AnyValue.newBuilder().setBoolValue(randomBoolean()).build()));
            case MIXED_NUMERIC_ARRAY -> {
                // Leading with a double maps the field to a float, which then widens the ints without loss.
                // The other order would map it to a long and silently truncate every double.
                List<AnyValue> elements = new ArrayList<>();
                elements.add(AnyValue.newBuilder().setDoubleValue(randomDouble()).build());
                elements.addAll(
                    randomElements(
                        () -> randomBoolean()
                            ? AnyValue.newBuilder().setIntValue(randomLong()).build()
                            : AnyValue.newBuilder().setDoubleValue(randomDouble()).build()
                    )
                );
                yield arrayValue(elements);
            }
            case ARRAY_WITH_NULLS -> {
                List<AnyValue> elements = new ArrayList<>();
                elements.add(stringValue(randomAlphaOfLengthBetween(1, 8)));
                elements.addAll(randomElements(() -> randomBoolean() ? stringValue(randomAlphaOfLengthBetween(1, 8)) : unsetValue()));
                yield arrayValue(elements);
            }
            case EMPTY_ARRAY -> arrayValue(List.of());
            case UNSET -> unsetValue();
        };
    }

    /** One to three elements, so that single-element and multi-valued arrays both occur. */
    private static List<AnyValue> randomElements(Supplier<AnyValue> element) {
        List<AnyValue> elements = new ArrayList<>();
        for (int i = 0, count = randomIntBetween(1, 3); i < count; i++) {
            elements.add(element.get());
        }
        return elements;
    }

    private static AnyValue stringValue(String value) {
        return AnyValue.newBuilder().setStringValue(value).build();
    }

    private static AnyValue arrayValue(List<AnyValue> elements) {
        return AnyValue.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(elements).build()).build();
    }

    private static AnyValue unsetValue() {
        return AnyValue.getDefaultInstance();
    }

    private static KeyValue stringAttribute(String key, String value) {
        return KeyValue.newBuilder().setKey(key).setValue(stringValue(value)).build();
    }

    /** The data points of one metric of one resource at a few consecutive timestamps. */
    private record Message(int resourceIdx, int metricIdx, long[] timestamps) {}

    /** Encodes {@code messages} as a single OTLP export and sends it to the data stream selected by {@code dataset}. */
    @FunctionalInterface
    private interface Sender {
        void send(String dataset, Workload workload, List<Message> messages) throws IOException;
    }

    /**
     * Sends {@code workload} to the data stream selected by {@code dataset}. Timestamps are grouped into
     * messages of {@code numTimestampsPerMsg}, and the metrics are distributed round-robin across
     * {@code numSplits} exports, each covering a disjoint subset of the metrics.
     *
     * @return the number of exports sent
     */
    private static int populate(Sender sender, String dataset, Workload workload, long[] timestamps, int numTimestampsPerMsg, int numSplits)
        throws IOException {
        List<List<Message>> splits = new ArrayList<>(numSplits);
        for (int s = 0; s < numSplits; s++) {
            splits.add(new ArrayList<>());
        }
        for (int msgStart = 0; msgStart < timestamps.length; msgStart += numTimestampsPerMsg) {
            long[] msgTimestamps = Arrays.copyOfRange(timestamps, msgStart, Math.min(msgStart + numTimestampsPerMsg, timestamps.length));
            for (int r = 0; r < workload.resourceAttributes().size(); r++) {
                for (int m = 0; m < workload.metrics().size(); m++) {
                    splits.get(m % numSplits).add(new Message(r, m, msgTimestamps));
                }
            }
        }
        int exports = 0;
        for (List<Message> split : splits) {
            if (split.isEmpty() == false) {
                sender.send(dataset, workload, split);
                exports++;
            }
        }
        return exports;
    }

    private static void createDataStream(String name) throws IOException {
        assertOK(client().performRequest(new Request("PUT", "_data_stream/" + name)));
    }

    /**
     * Wraps {@code metric} in the resource and scope for {@code resourceIdx}. The {@code data_stream.dataset}
     * resource attribute is the only difference between the payloads the two streams receive.
     */
    private static ResourceMetrics buildResourceMetrics(String dataset, Workload workload, int resourceIdx, Metric metric) {
        Resource resource = Resource.newBuilder()
            .addAllAttributes(workload.resourceAttributes().get(resourceIdx))
            .addAttributes(stringAttribute("data_stream.dataset", dataset))
            .build();
        InstrumentationScope scope = InstrumentationScope.newBuilder()
            .setName(SCOPE_NAME)
            .setVersion(SCOPE_VERSION)
            .addAllAttributes(workload.scopeAttributes())
            .build();
        return ResourceMetrics.newBuilder()
            .setResource(resource)
            .addScopeMetrics(ScopeMetrics.newBuilder().setScope(scope).addMetrics(metric).build())
            .build();
    }

    /** Builds one metric carrying one data point per (attribute set, timestamp) pair. */
    private static Metric buildMetric(Workload workload, int metricIdx, int resourceIdx, long[] msgTimestamps) {
        MetricSpec spec = workload.metrics().get(metricIdx);
        List<List<KeyValue>> attributeSets = workload.dataPointAttributes().get(metricIdx);
        List<NumberDataPoint> points = new ArrayList<>(attributeSets.size() * msgTimestamps.length);
        for (long timestamp : msgTimestamps) {
            for (int a = 0; a < attributeSets.size(); a++) {
                NumberDataPoint.Builder point = NumberDataPoint.newBuilder()
                    .setTimeUnixNano(timestamp)
                    .setStartTimeUnixNano(timestamp)
                    .addAllAttributes(attributeSets.get(a));
                long value = pointValue(resourceIdx, a, metricIdx);
                points.add(spec.longValued() ? point.setAsInt(value).build() : point.setAsDouble(value + 0.5).build());
            }
        }
        Metric.Builder metric = Metric.newBuilder().setName(spec.name()).setUnit("1");
        return spec.sum()
            ? metric.setSum(
                Sum.newBuilder()
                    .addAllDataPoints(points)
                    .setIsMonotonic(true)
                    .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                    .build()
            ).build()
            : metric.setGauge(Gauge.newBuilder().addAllDataPoints(points).build()).build();
    }

    /** Point values are derived from the indices rather than randomised, so a monotonic sum never goes negative. */
    private static long pointValue(int resourceIdx, int attributeSetIdx, int metricIdx) {
        return resourceIdx * 10_000L + attributeSetIdx * 100L + metricIdx;
    }

    /** Builds the export as OTLP protobuf directly, wrapping every message in its own resource and scope. */
    private static void exportProto(String dataset, Workload workload, List<Message> messages) throws IOException {
        ExportMetricsServiceRequest.Builder metricsRequest = ExportMetricsServiceRequest.newBuilder();
        for (Message message : messages) {
            Metric metric = buildMetric(workload, message.metricIdx(), message.resourceIdx(), message.timestamps());
            metricsRequest.addResourceMetrics(buildResourceMetrics(dataset, workload, message.resourceIdx(), metric));
        }
        Request request = new Request("POST", "/_otlp/v1/metrics");
        request.setEntity(new ByteArrayEntity(metricsRequest.build().toByteArray(), ContentType.create("application/x-protobuf")));
        Response response = client().performRequest(request);
        assertOK(response);
        ExportMetricsServiceResponse metricsResponse = ExportMetricsServiceResponse.parseFrom(response.getEntity().getContent());
        assertThat(
            "OTLP export must not reject any data point: " + metricsResponse.getPartialSuccess().getErrorMessage(),
            metricsResponse.getPartialSuccess().getRejectedDataPoints(),
            equalTo(0L)
        );
    }

    /**
     * Sends the export through the OpenTelemetry SDK's exporter, which groups all metrics of a resource and
     * scope into one message. The SDK only reports partial success in its own log, so rejected data points
     * surface as missing documents instead.
     */
    private static void exportSdk(String dataset, Workload workload, List<Message> messages) {
        InstrumentationScopeInfo scope = InstrumentationScopeInfo.builder(SCOPE_NAME)
            .setVersion(SCOPE_VERSION)
            .setAttributes(sdkAttributes(workload.scopeAttributes()))
            .build();
        List<MetricData> metrics = new ArrayList<>(messages.size());
        for (Message message : messages) {
            metrics.add(buildSdkMetric(dataset, workload, scope, message));
        }
        CompletableResultCode result = sdkExporter.export(metrics).join(30, TimeUnit.SECONDS);
        assertTrue("SDK export must succeed", result.isSuccess());
    }

    /** The SDK counterpart of {@link #buildResourceMetrics} and {@link #buildMetric}. */
    private static MetricData buildSdkMetric(String dataset, Workload workload, InstrumentationScopeInfo scope, Message message) {
        MetricSpec spec = workload.metrics().get(message.metricIdx());
        io.opentelemetry.sdk.resources.Resource resource = io.opentelemetry.sdk.resources.Resource.create(
            sdkAttributes(workload.resourceAttributes().get(message.resourceIdx())).toBuilder().put("data_stream.dataset", dataset).build()
        );
        List<List<KeyValue>> attributeSets = workload.dataPointAttributes().get(message.metricIdx());
        if (spec.longValued()) {
            List<LongPointData> points = new ArrayList<>();
            for (long timestamp : message.timestamps()) {
                for (int a = 0; a < attributeSets.size(); a++) {
                    long value = pointValue(message.resourceIdx(), a, message.metricIdx());
                    points.add(ImmutableLongPointData.create(timestamp, timestamp, sdkAttributes(attributeSets.get(a)), value));
                }
            }
            return spec.sum()
                ? ImmutableMetricData.createLongSum(
                    resource,
                    scope,
                    spec.name(),
                    "",
                    "1",
                    ImmutableSumData.create(true, CUMULATIVE, points)
                )
                : ImmutableMetricData.createLongGauge(resource, scope, spec.name(), "", "1", ImmutableGaugeData.create(points));
        }
        List<DoublePointData> points = new ArrayList<>();
        for (long timestamp : message.timestamps()) {
            for (int a = 0; a < attributeSets.size(); a++) {
                double value = pointValue(message.resourceIdx(), a, message.metricIdx()) + 0.5;
                points.add(ImmutableDoublePointData.create(timestamp, timestamp, sdkAttributes(attributeSets.get(a)), value));
            }
        }
        return spec.sum()
            ? ImmutableMetricData.createDoubleSum(resource, scope, spec.name(), "", "1", ImmutableSumData.create(true, CUMULATIVE, points))
            : ImmutableMetricData.createDoubleGauge(resource, scope, spec.name(), "", "1", ImmutableGaugeData.create(points));
    }

    /** Converts protobuf attributes to SDK {@link Attributes}; only {@link #SDK_SHAPES} can be converted. */
    private static Attributes sdkAttributes(List<KeyValue> attributes) {
        AttributesBuilder builder = Attributes.builder();
        for (KeyValue kv : attributes) {
            AnyValue value = kv.getValue();
            switch (value.getValueCase()) {
                case STRING_VALUE -> builder.put(kv.getKey(), value.getStringValue());
                case INT_VALUE -> builder.put(kv.getKey(), value.getIntValue());
                case DOUBLE_VALUE -> builder.put(kv.getKey(), value.getDoubleValue());
                case BOOL_VALUE -> builder.put(kv.getKey(), value.getBoolValue());
                case ARRAY_VALUE -> putSdkArray(builder, kv.getKey(), value.getArrayValue().getValuesList());
                case BYTES_VALUE, KVLIST_VALUE, VALUE_NOT_SET -> throw new IllegalArgumentException("not expressible in the SDK: " + kv);
            }
        }
        return builder.build();
    }

    /** Puts a non-empty, homogeneous array, typed by its first element. */
    private static void putSdkArray(AttributesBuilder builder, String key, List<AnyValue> elements) {
        switch (elements.getFirst().getValueCase()) {
            case STRING_VALUE -> builder.put(AttributeKey.stringArrayKey(key), elements.stream().map(AnyValue::getStringValue).toList());
            case INT_VALUE -> builder.put(AttributeKey.longArrayKey(key), elements.stream().map(AnyValue::getIntValue).toList());
            case DOUBLE_VALUE -> builder.put(AttributeKey.doubleArrayKey(key), elements.stream().map(AnyValue::getDoubleValue).toList());
            case BOOL_VALUE -> builder.put(AttributeKey.booleanArrayKey(key), elements.stream().map(AnyValue::getBoolValue).toList());
            case ARRAY_VALUE, BYTES_VALUE, KVLIST_VALUE, VALUE_NOT_SET -> throw new IllegalArgumentException(
                "not expressible in the SDK: " + key + "=" + elements
            );
        }
    }

    private static void refreshAll() throws IOException {
        assertOK(client().performRequest(new Request("POST", "metrics-*.otel-default/_refresh")));
    }

    /**
     * Installs an index template that overrides {@code metrics-otel@template} (priority 120) for the given
     * pattern at priority 200. Uses the same {@code composed_of} list as the managed OTel template so field
     * mappings are identical.
     *
     * @param batchIndexing when {@code true}, adds {@code index.batch_indexing: true} to the
     *                      template settings so that backing indices opt in to the ESCF columnar path.
     */
    private static void installTemplate(String name, String pattern, boolean batchIndexing) throws IOException {
        // Disable ILM to prevent lazy rollover during the test window (which would switch the
        // write index and cause past timestamps to be rejected as out-of-range for the new index).
        String batchIndexingSetting = batchIndexing ? """
            ,
                  "index.batch_indexing": true""" : "";
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

    /**
     * Returns the mappings of the single backing index of {@code target}, with the {@code data_stream.dataset}
     * constant_keyword removed: its value is the stream's own dataset name, which the two streams
     * intentionally disagree on. Every other field is derived from the documents and must match.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> indexMappings(String target) throws IOException {
        Map<String, Object> response = ObjectPath.createFromResponse(client().performRequest(new Request("GET", target + "/_mapping")))
            .evaluate("");
        assertThat("exactly one backing index is expected for " + target, response.size(), equalTo(1));
        Map<String, Object> mappings = (Map<String, Object>) ((Map<String, Object>) response.values().iterator().next()).get("mappings");
        // The template declares data_stream.dataset as a dotted property, which the mapper expands into an
        // object, so the leaf to drop sits at properties.data_stream.properties.dataset.
        Map<String, Object> copy = new LinkedHashMap<>(mappings);
        Map<String, Object> properties = new LinkedHashMap<>((Map<String, Object>) copy.get("properties"));
        Map<String, Object> dataStream = new LinkedHashMap<>((Map<String, Object>) properties.get("data_stream"));
        Map<String, Object> dataStreamProperties = new LinkedHashMap<>((Map<String, Object>) dataStream.get("properties"));
        assertNotNull("data_stream.dataset must be mapped", dataStreamProperties.remove("dataset"));
        dataStream.put("properties", dataStreamProperties);
        properties.put("data_stream", dataStream);
        copy.put("properties", properties);
        return copy;
    }

    /**
     * Returns a copy of {@code source} with the {@code data_stream.dataset} leaf removed.
     * {@code data_stream.dataset} is a constant_keyword whose value is the data-stream's own dataset name,
     * which is intentionally different between the two streams under comparison. All other source fields
     * must be equal.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> stripDataStreamDataset(Map<String, Object> source) {
        Map<String, Object> result = new LinkedHashMap<>(source);
        Object dsRaw = result.get("data_stream");
        if (dsRaw instanceof Map) {
            Map<String, Object> ds = new LinkedHashMap<>((Map<String, Object>) dsRaw);
            ds.remove("dataset");
            result.put("data_stream", ds);
        }
        return result;
    }

    /**
     * Fetches all documents from {@code target}, sorted by {@code @timestamp} ASC then {@code _tsid} ASC.
     * Each element in the returned list is a map with keys {@code _id}, {@code _tsid}, and {@code _source}.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> fetchSorted(String target) throws IOException {
        Request request = new Request("GET", target + "/_search");
        request.setJsonEntity("""
            {
              "size": 10000,
              "track_total_hits": true,
              "sort": [{"@timestamp": "asc"}, {"_tsid": "asc"}],
              "docvalue_fields": ["_tsid"],
              "_source": true
            }
            """);
        ObjectPath response = ObjectPath.createFromResponse(client().performRequest(request));
        List<Object> rawHits = response.evaluate("hits.hits");
        int totalHits = response.evaluate("hits.total.value");
        assertThat("all documents of " + target + " must fit in one page", rawHits.size(), equalTo(totalHits));
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
