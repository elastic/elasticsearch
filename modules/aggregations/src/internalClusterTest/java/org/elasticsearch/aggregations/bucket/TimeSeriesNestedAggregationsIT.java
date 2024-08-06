/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.aggregations.AggregationIntegTestCase;
import org.elasticsearch.aggregations.bucket.timeseries.InternalTimeSeries;
import org.elasticsearch.aggregations.bucket.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class TimeSeriesNestedAggregationsIT extends AggregationIntegTestCase {
    private static int numberOfDimensions;
    private static int numberOfDocuments;

    private static final String FOO_DIM_VALUE = "foo".repeat(10);
    private static final String BAR_DIM_VALUE = "bar".repeat(11);
    private static final String BAZ_DIM_VALUE = "baz".repeat(12);

    @Before
    public void setup() throws Exception {
        numberOfDimensions = randomIntBetween(10, 20);
        final XContentBuilder mapping = timeSeriesIndexMapping();
        long startMillis = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        long endMillis = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-31T00:00:00Z");
        numberOfDocuments = randomIntBetween(100, 200);
        final Iterator<Long> timestamps = getTimestamps(startMillis, endMillis, numberOfDocuments);
        // NOTE: use also the last (changing) dimension so to make sure documents are not indexed all in the same shard.
        final String[] routingDimensions = new String[] { "dim_000000", formatDim(numberOfDimensions - 1) };
        assertTrue(prepareTimeSeriesIndex(mapping, startMillis, endMillis, routingDimensions).isAcknowledged());
        logger.info("Dimensions: " + numberOfDimensions + " docs: " + numberOfDocuments + " start: " + startMillis + " end: " + endMillis);

        final BulkRequestBuilder bulkIndexRequest = client().prepareBulk();
        for (int docId = 0; docId < numberOfDocuments; docId++) {
            final XContentBuilder document = timeSeriesDocument(FOO_DIM_VALUE, BAR_DIM_VALUE, BAZ_DIM_VALUE, docId, timestamps::next);
            bulkIndexRequest.add(prepareIndex("index").setOpType(DocWriteRequest.OpType.CREATE).setSource(document));
        }

        final BulkResponse bulkIndexResponse = bulkIndexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertFalse(bulkIndexResponse.hasFailures());
        assertEquals(RestStatus.OK.getStatus(), client().admin().indices().prepareFlush("index").get().getStatus().getStatus());
    }

    private static XContentBuilder timeSeriesDocument(
        final String fooDimValue,
        final String barDimValue,
        final String bazDimValue,
        int docId,
        final Supplier<Long> timestampSupplier
    ) throws IOException {
        final XContentBuilder docSource = XContentFactory.jsonBuilder();
        docSource.startObject();
        // NOTE: we assign dimensions in such a way that almost all of them have the same value but the last one.
        // This way we are going to have just two time series (and two distinct tsid) and the last dimension identifies
        // which time series the document belongs to.
        for (int dimId = 0; dimId < numberOfDimensions - 1; dimId++) {
            docSource.field(formatDim(dimId), fooDimValue);
        }
        docSource.field(formatDim(numberOfDimensions - 1), docId % 2 == 0 ? barDimValue : bazDimValue);
        docSource.field("counter_metric", docId + 1);
        docSource.field("gauge_metric", randomDoubleBetween(1000.0, 2000.0, true));
        docSource.field("@timestamp", timestampSupplier.get());
        docSource.endObject();

        return docSource;
    }

    private CreateIndexResponse prepareTimeSeriesIndex(
        final XContentBuilder mapping,
        long startMillis,
        long endMillis,
        final String[] routingDimensions
    ) {
        return prepareCreate("index").setSettings(
            indexSettings(randomIntBetween(1, 3), randomIntBetween(1, 3)).put("mode", "time_series")
                .put("routing_path", String.join(",", routingDimensions))
                .put("time_series.start_time", startMillis)
                .put("time_series.end_time", endMillis)
                .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), 4192)
        ).setMapping(mapping).get();
    }

    private static Iterator<Long> getTimestamps(long startMillis, long endMillis, int numberOfDocs) {
        final Set<Long> timestamps = new TreeSet<>();
        while (timestamps.size() < numberOfDocs) {
            timestamps.add(randomLongBetween(startMillis, endMillis));
        }
        return timestamps.iterator();
    }

    private static XContentBuilder timeSeriesIndexMapping() throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("properties");
        for (int i = 0; i < numberOfDimensions; i++) {
            builder.startObject(formatDim(i));
            builder.field("type", "keyword");
            builder.field("time_series_dimension", true);
            builder.endObject();
        }
        builder.startObject("counter_metric");
        builder.field("type", "double");
        builder.field("time_series_metric", "counter");
        builder.endObject();
        builder.startObject("gauge_metric");
        builder.field("type", "double");
        builder.field("time_series_metric", "gauge");
        builder.endObject();
        builder.endObject(); // properties
        builder.endObject();
        return builder;
    }

    private static String formatDim(int dimId) {
        return String.format(Locale.ROOT, "dim_%06d", dimId);
    }

    public void testTimeSeriesAggregation() {
        final TimeSeriesAggregationBuilder timeSeries = new TimeSeriesAggregationBuilder("ts");
        assertResponse(prepareSearch("index").addAggregation(timeSeries).setSize(0), response -> {
            final InternalTimeSeries ts = (InternalTimeSeries) response.getAggregations().asList().get(0);
            assertTimeSeriesAggregation(ts);
        });
    }

    public void testSumByTsid() {
        final TimeSeriesAggregationBuilder timeSeries = new TimeSeriesAggregationBuilder("ts").subAggregation(
            new SumAggregationBuilder("sum").field("gauge_metric")
        );
        assertResponse(
            prepareSearch("index").setQuery(new MatchAllQueryBuilder()),
            response -> assertNotEquals(numberOfDocuments, response.getHits().getHits().length)
        );

        assertResponse(prepareSearch("index").addAggregation(timeSeries).setSize(0), response -> {
            final InternalTimeSeries ts = (InternalTimeSeries) response.getAggregations().asList().get(0);
            assertTimeSeriesAggregation(ts);
        });
    }

    public void testTermsByTsid() {
        final TimeSeriesAggregationBuilder timeSeries = new TimeSeriesAggregationBuilder("ts").subAggregation(
            new TermsAggregationBuilder("terms").field("dim_0")
        );
        assertResponse(prepareSearch("index").addAggregation(timeSeries).setSize(0), response -> {
            final InternalTimeSeries ts = (InternalTimeSeries) response.getAggregations().asList().get(0);
            assertTimeSeriesAggregation(ts);
        });
    }

    public void testDateHistogramByTsid() {
        final TimeSeriesAggregationBuilder timeSeries = new TimeSeriesAggregationBuilder("ts").subAggregation(
            new DateHistogramAggregationBuilder("date_histogram").field("@timestamp").calendarInterval(DateHistogramInterval.HOUR)
        );
        assertResponse(prepareSearch("index").addAggregation(timeSeries).setSize(0), response -> {
            final InternalTimeSeries ts = (InternalTimeSeries) response.getAggregations().asList().get(0);
            assertTimeSeriesAggregation(ts);
        });
    }

    public void testCardinalityByTsid() {
        final TimeSeriesAggregationBuilder timeSeries = new TimeSeriesAggregationBuilder("ts").subAggregation(
            new CardinalityAggregationBuilder("dim_n_cardinality").field(formatDim(numberOfDimensions - 1))
        );
        assertResponse(prepareSearch("index").addAggregation(timeSeries).setSize(0), response -> {
            final InternalTimeSeries ts = (InternalTimeSeries) response.getAggregations().asList().get(0);
            assertTimeSeriesAggregation(ts);
            ts.getBuckets().forEach(bucket -> { assertCardinality(bucket.getAggregations().get("dim_n_cardinality"), 1); });
        });
    }

    private static void assertTimeSeriesAggregation(final InternalTimeSeries timeSeriesAggregation) {
        final var dimensions = timeSeriesAggregation.getBuckets().stream().map(InternalTimeSeries.InternalBucket::getKey).toList();
        // NOTE: only two time series expected as a result of having just two distinct values for the last dimension
        assertEquals(2, dimensions.size());

        final Object firstTimeSeries = dimensions.get(0);
        final Object secondTimeSeries = dimensions.get(1);

        assertNotEquals(firstTimeSeries, secondTimeSeries);
    }

    private static void assertCardinality(final InternalCardinality cardinalityAggregation, int expectedCardinality) {
        assertEquals(expectedCardinality, cardinalityAggregation.getValue());
    }
}
