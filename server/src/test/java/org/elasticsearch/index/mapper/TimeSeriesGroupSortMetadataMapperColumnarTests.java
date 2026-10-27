/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Columnar ↔ x-content parity for {@link MetricNameHashFieldMapper} and {@link DimensionGroupFieldMapper}, which both derive their
 * values from the dimensions of a document: on the row path from the parsed Lucene document, on the columnar path from the source
 * columns. Uses the coordinator-tsid strategy ({@code index.dimensions}) with pre-supplied tsids, as the columnar path requires.
 */
public class TimeSeriesGroupSortMetadataMapperColumnarTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final BytesRef TSID = new BytesRef(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
    private static final int ROUTING_HASH = 42;
    private static final String ROUTING = TimeSeriesRoutingHashFieldMapper.encode(ROUTING_HASH);
    private static final long TS = 1705320000000L;

    private static Settings settings(String dimensionGroupField) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim.*", "labels.*", "_metric_names_hash")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_DIMENSION_GROUP_FIELD.getKey(), dimensionGroupField)
            // the documents carry ids created by TsidExtractingIdFieldMapper#createId, not synthetic ones
            .put(IndexSettings.SYNTHETIC_ID.getKey(), false)
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    private static XContentBuilder metricsMapping() throws IOException {
        return mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("_metric_names_hash").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("labels").startObject("properties");
            b.startObject("__name__").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
            b.startObject("dim").startObject("properties");
            b.startObject("host").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("region").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("pod").field("type", "long").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        });
    }

    private static Doc tsdbDoc(int seqNo, String fields) {
        long ts = TS + seqNo;
        String id = TsidExtractingIdFieldMapper.createId(ROUTING_HASH, TSID, ts);
        return doc(id, ROUTING, TSID, seqNo, "{\"@timestamp\":" + ts + "," + fields + "}");
    }

    public void testOtelMetricNamesHashAndHostGroup() throws IOException {
        assertColumnarMatchesXContent(
            metricsMapping(),
            settings("dim.host"),
            batch(
                "otel",
                1L,
                tsdbDoc(1, "\"_metric_names_hash\":\"abc123\",\"dim\":{\"host\":\"h1\",\"region\":\"eu\"}"),
                tsdbDoc(2, "\"_metric_names_hash\":\"abc123\",\"dim\":{\"host\":\"h2\"}"),
                tsdbDoc(3, "\"_metric_names_hash\":\"def456\",\"dim\":{\"region\":\"us\"}"),
                tsdbDoc(4, "\"_metric_names_hash\":\"def456\",\"dim\":{\"host\":[\"h3\",\"h4\"]}")
            )
        );
    }

    public void testPrometheusMetricName() throws IOException {
        assertColumnarMatchesXContent(
            metricsMapping(),
            settings("labels.job"),
            batch(
                "prometheus",
                1L,
                tsdbDoc(1, "\"labels\":{\"__name__\":\"http_requests_total\",\"job\":\"api\"}"),
                tsdbDoc(2, "\"labels\":{\"__name__\":\"http_requests_total\",\"job\":\"web\"}"),
                tsdbDoc(3, "\"labels\":{\"__name__\":\"process_cpu_seconds\"}")
            )
        );
    }

    /** Rows without a metric name field fall back to the hash of their dimension names, on both paths. */
    public void testDimensionNamesFallback() throws IOException {
        assertColumnarMatchesXContent(
            metricsMapping(),
            settings("dim.host"),
            batch(
                "fallback",
                1L,
                tsdbDoc(1, "\"dim\":{\"host\":\"h1\",\"region\":\"eu\"}"),
                tsdbDoc(2, "\"dim\":{\"host\":\"h1\"}"),
                tsdbDoc(3, "\"dim\":{\"host\":[\"h1\",\"h2\"],\"region\":\"eu\"}"),
                tsdbDoc(4, "\"dim\":{\"host\":\"h1\",\"pod\":7}"),
                tsdbDoc(5, "\"dim\":{\"region\":\"us\"}")
            )
        );
    }

    /** A batch mixing rows with and without the metric name field. */
    public void testMixedRows() throws IOException {
        assertColumnarMatchesXContent(
            metricsMapping(),
            settings("dim.host"),
            batch(
                "mixed",
                1L,
                tsdbDoc(1, "\"_metric_names_hash\":\"abc123\",\"dim\":{\"host\":\"h1\"}"),
                tsdbDoc(2, "\"dim\":{\"host\":\"h1\",\"region\":\"eu\"}"),
                tsdbDoc(3, "\"labels\":{\"__name__\":\"up\"},\"dim\":{\"host\":\"h2\"}"),
                tsdbDoc(4, "\"dim\":{\"pod\":3}")
            )
        );
    }

    /** A numeric group field yields no {@code _dimension_group} on either path. */
    public void testNumericGroupField() throws IOException {
        assertColumnarMatchesXContent(
            metricsMapping(),
            settings("dim.pod"),
            batch("numeric group", 1L, tsdbDoc(1, "\"dim\":{\"host\":\"h1\",\"pod\":7}"), tsdbDoc(2, "\"dim\":{\"host\":\"h2\"}"))
        );
    }
}
