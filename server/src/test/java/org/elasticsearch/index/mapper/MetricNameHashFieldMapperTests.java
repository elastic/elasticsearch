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
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Scenarios for {@code _metric_name_hash} on the row path: the OTel metric names hash wins, then the Prometheus metric name, and
 * otherwise the hash only depends on the set of dimension names of the document. Covers both tsid strategies, since the fallback
 * reads the dimension names from a different place for each.
 */
public class MetricNameHashFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return MetricNameHashFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // There aren't any parameters
    }

    @Override
    protected Settings getIndexSettings() {
        return routingPathSettings().build();
    }

    private static Settings.Builder routingPathSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim.*,labels.*,_metric_names_hash");
    }

    private static Settings.Builder indexDimensionsSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim.*,labels.*,_metric_names_hash");
    }

    /** Both tsid strategies: {@code index.routing_path} (dimension names collected while parsing) and {@code index.dimensions}. */
    private static Settings.Builder randomStrategySettings() {
        return randomBoolean() ? routingPathSettings() : indexDimensionsSettings();
    }

    private DocumentMapper createMapper(Settings.Builder settings, XContentBuilder mappings) throws IOException {
        return createMapper(getVersion(), settings, mappings);
    }

    private DocumentMapper createMapper(IndexVersion version, Settings.Builder settings, XContentBuilder mappings) throws IOException {
        return createMapperService(version, settings.build(), () -> true, mappings).documentMapper();
    }

    private static XContentBuilder metricsMapping() throws IOException {
        return mapping(b -> {
            b.startObject("_metric_names_hash").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("labels").field("type", "passthrough").field("priority", 1).field("time_series_dimension", true).endObject();
            b.startObject("dim").startObject("properties");
            b.startObject("host").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("region").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("pod").field("type", "long").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        });
    }

    private ParsedDocument parseDocument(DocumentMapper docMapper, CheckedConsumer<XContentBuilder, IOException> f) throws IOException {
        // the routing hash is computed on the coordinating node and travels as the routing of the index request
        return docMapper.parse(source(null, b -> {
            f.accept(b);
            b.field("@timestamp", "2021-10-01");
        }, TimeSeriesRoutingHashFieldMapper.encode(randomInt())));
    }

    private static BytesRef metricNameHash(ParsedDocument doc) {
        return doc.rootDoc().getBinaryValue(MetricNameHashFieldMapper.NAME);
    }

    private static BytesRef expectedHashOf(String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        return MetricNameHashFieldMapper.encode(MetricNameHashFieldMapper.hashValue(utf8, 0, utf8.length, new MurmurHash3.Hash128()));
    }

    public void testOtelMetricNamesHash() throws Exception {
        DocumentMapper docMapper = createMapper(randomStrategySettings(), metricsMapping());
        assertThat(docMapper.metadataMapper(MetricNameHashFieldMapper.class), is(MetricNameHashFieldMapper.INSTANCE));
        ParsedDocument doc = parseDocument(
            docMapper,
            b -> b.field("_metric_names_hash", "abc123").startObject("dim").field("host", "h1").endObject()
        );
        assertThat(metricNameHash(doc), equalTo(expectedHashOf("abc123")));
        assertThat(metricNameHash(doc).length, equalTo(Long.BYTES));

        // same metrics, other dimensions: same hash
        ParsedDocument other = parseDocument(
            docMapper,
            b -> b.field("_metric_names_hash", "abc123").startObject("dim").field("host", "h2").field("region", "eu").endObject()
        );
        assertThat(metricNameHash(other), equalTo(metricNameHash(doc)));
        // other metrics: other hash
        ParsedDocument otherMetric = parseDocument(
            docMapper,
            b -> b.field("_metric_names_hash", "def456").startObject("dim").field("host", "h1").endObject()
        );
        assertThat(metricNameHash(otherMetric), not(equalTo(metricNameHash(doc))));
    }

    public void testPrometheusMetricName() throws Exception {
        DocumentMapper docMapper = createMapper(randomStrategySettings(), metricsMapping());
        ParsedDocument doc = parseDocument(
            docMapper,
            b -> b.startObject("labels").field("__name__", "http_requests_total").field("job", "api").endObject()
        );
        assertThat(metricNameHash(doc), equalTo(expectedHashOf("http_requests_total")));
    }

    public void testOtelTakesPrecedenceOverPrometheus() throws Exception {
        DocumentMapper docMapper = createMapper(randomStrategySettings(), metricsMapping());
        ParsedDocument doc = parseDocument(
            docMapper,
            b -> b.field("_metric_names_hash", "abc123").startObject("labels").field("__name__", "http_requests_total").endObject()
        );
        assertThat(metricNameHash(doc), equalTo(expectedHashOf("abc123")));
    }

    /** Without a metric name field the hash depends on the dimension names, not on their values nor on non-dimension fields. */
    public void testFallbackToDimensionNames() throws Exception {
        for (Settings.Builder settings : new Settings.Builder[] { routingPathSettings(), indexDimensionsSettings() }) {
            DocumentMapper docMapper = createMapper(settings, metricsMapping());
            ParsedDocument hostRegion = parseDocument(
                docMapper,
                b -> b.startObject("dim").field("host", "h1").field("region", "eu").endObject()
            );
            assertThat(metricNameHash(hostRegion), notNullValue());
            assertThat(metricNameHash(hostRegion).length, equalTo(Long.BYTES));

            ParsedDocument otherValues = parseDocument(
                docMapper,
                b -> b.startObject("dim").field("region", "us").field("host", "h2").endObject().field("other", "ignored")
            );
            assertThat(metricNameHash(otherValues), equalTo(metricNameHash(hostRegion)));

            ParsedDocument arrayValues = parseDocument(
                docMapper,
                b -> b.startObject("dim").array("host", "h1", "h2").field("region", "eu").endObject()
            );
            assertThat(metricNameHash(arrayValues), equalTo(metricNameHash(hostRegion)));

            ParsedDocument hostOnly = parseDocument(docMapper, b -> b.startObject("dim").field("host", "h1").endObject());
            assertThat(metricNameHash(hostOnly), not(equalTo(metricNameHash(hostRegion))));

            ParsedDocument hostPod = parseDocument(docMapper, b -> b.startObject("dim").field("host", "h1").field("pod", 7).endObject());
            assertThat(metricNameHash(hostPod), not(equalTo(metricNameHash(hostRegion))));
            assertThat(metricNameHash(hostPod), not(equalTo(metricNameHash(hostOnly))));
        }
    }

    /** The fallback value is the same for both tsid strategies, so a routing_path and an index.dimensions index agree. */
    public void testFallbackAgreesAcrossStrategies() throws Exception {
        DocumentMapper routingPath = createMapper(routingPathSettings(), metricsMapping());
        DocumentMapper indexDimensions = createMapper(indexDimensionsSettings(), metricsMapping());
        CheckedConsumer<XContentBuilder, IOException> source = b -> b.startObject("dim")
            .field("host", "h1")
            .field("region", "eu")
            .endObject();
        assertThat(metricNameHash(parseDocument(routingPath, source)), equalTo(metricNameHash(parseDocument(indexDimensions, source))));
    }

    public void testDisabledWithEmptyGroupField() throws Exception {
        Settings.Builder settings = randomStrategySettings().put(IndexSettings.TIME_SERIES_DIMENSION_GROUP_FIELD.getKey(), "");
        DocumentMapper docMapper = createMapper(settings, metricsMapping());
        assertThat(docMapper.metadataMapper(MetricNameHashFieldMapper.class), is(nullValue()));
        ParsedDocument doc = parseDocument(docMapper, b -> b.field("_metric_names_hash", "abc123"));
        assertThat(metricNameHash(doc), nullValue());
    }

    public void testDisabledBeforeVersion() throws Exception {
        IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.TSID_SINGLE_PREFIX_BYTE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_DIMENSION_GROUP_SORT)
        );
        DocumentMapper docMapper = createMapper(version, randomStrategySettings(), metricsMapping());
        assertThat(docMapper.metadataMapper(MetricNameHashFieldMapper.class), is(nullValue()));
    }

    public void testDisabledInStandardMode() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.name()).build();
        DocumentMapper docMapper = createMapperService(settings, mapping(b -> {})).documentMapper();
        assertThat(docMapper.metadataMapper(MetricNameHashFieldMapper.class), is(nullValue()));
    }

    public void testIncludeInDocumentNotAllowed() throws Exception {
        DocumentMapper docMapper = createMapper(randomStrategySettings(), metricsMapping());
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> parseDocument(docMapper, b -> b.startObject("dim").field("host", "h1").endObject().field("_metric_name_hash", "foo"))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("Field [_metric_name_hash] is a metadata field and cannot be added inside a document")
        );
    }
}
