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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.aggregations.AggregationIntegTestCase;
import org.elasticsearch.aggregations.bucket.timeseries.InternalTimeSeries;
import org.elasticsearch.aggregations.bucket.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

@ESIntegTestCase.ClusterScope(numDataNodes = 3, numClientNodes = 1, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class TimeSeriesAggregationsUnlimitedDimensionsIT extends AggregationIntegTestCase {
    private static int numberOfDimensions;
    private static int numberOfDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        numberOfDimensions = randomIntBetween(25, 99);
        final XContentBuilder mapping = timeSeriesIndexMapping();
        long startMillis = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        long endMillis = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-02T00:00:00Z");
        numberOfDocs = randomIntBetween(1000, 2000);
        final Iterator<Long> timestamps = getTimestamps(startMillis, endMillis, numberOfDocs);
        // NOTE: use also the last (changing) dimension so to make sure documents are not indexed all in the same shard.
        final String[] routingDimensions = new String[] { "dim_1", "dim_" + (numberOfDimensions - 1) };
        assertTrue(prepareTimeSeriesIndex(mapping, startMillis, endMillis, routingDimensions).isAcknowledged());

        logger.info("Dimensions: " + numberOfDimensions + " docs: " + numberOfDocs + " start: " + startMillis + " end: " + endMillis);

        // NOTE: we need the tsid to be larger than 32 Kb
        final String fooDimValue = "foo_" + randomAlphaOfLengthBetween(2048, 2048 + 128);
        final String barDimValue = "bar_" + randomAlphaOfLengthBetween(2048, 2048 + 256);
        final String bazDimValue = "baz_" + randomAlphaOfLengthBetween(2048, 2048 + 512);

        final BulkRequestBuilder bulkIndexRequest = client().prepareBulk();
        for (int docId = 0; docId < numberOfDocs; docId++) {
            final XContentBuilder document = timeSeriesDocument(fooDimValue, barDimValue, bazDimValue, docId, timestamps::next);
            bulkIndexRequest.add(client().prepareIndex("index").setOpType(DocWriteRequest.OpType.CREATE).setSource(document));
        }
        BulkResponse bulkIndexResponse = bulkIndexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
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
            docSource.field("dim_" + dimId, fooDimValue);
        }
        docSource.field("dim_" + (numberOfDimensions - 1), docId % 2 == 0 ? barDimValue : bazDimValue);
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
            Settings.builder()
                .put("mode", "time_series")
                .put("routing_path", String.join(",", routingDimensions))
                .put("index.number_of_shards", randomIntBetween(1, 3))
                .put("index.number_of_replicas", randomIntBetween(1, 3))
                .put("time_series.start_time", startMillis)
                .put("time_series.end_time", endMillis)
                .put(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING.getKey(), numberOfDimensions + 1)
                .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), 4192)
                .build()
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
            builder.startObject("dim_" + i);
            builder.field("type", "keyword");
            builder.field("time_series_dimension", true);
            builder.endObject();
        }
        builder.startObject("counter_metric");
        builder.field("type", "double");
        builder.endObject();
        builder.startObject("gauge_metric");
        builder.field("type", "double");
        builder.endObject();
        builder.endObject(); // properties
        builder.endObject();
        return builder;
    }

    public void testTimeSeriesAggregation() {
        final TimeSeriesAggregationBuilder timeSeries = new TimeSeriesAggregationBuilder("ts");
        final SearchResponse aggregationResponse = client().prepareSearch("index").addAggregation(timeSeries).setSize(0).get();
        final InternalTimeSeries ts = (InternalTimeSeries) aggregationResponse.getAggregations().asList().get(0);
        assertTimeSeriesAggregation(ts);
    }

    public void testSumByTsid() {
        final TimeSeriesAggregationBuilder timeSeries = new TimeSeriesAggregationBuilder("ts").subAggregation(
            new SumAggregationBuilder("sum").field("gauge_metric")
        );
        final SearchResponse searchResponse = client().prepareSearch("index").setQuery(new MatchAllQueryBuilder()).get();
        assertNotEquals(numberOfDocs, searchResponse.getHits().getHits().length);
        final SearchResponse aggregationResponse = client().prepareSearch("index").addAggregation(timeSeries).setSize(0).get();
        final InternalTimeSeries ts = (InternalTimeSeries) aggregationResponse.getAggregations().asList().get(0);
        assertTimeSeriesAggregation(ts);
    }

    public void testTermsByTsid() {
        final TimeSeriesAggregationBuilder timeSeries = new TimeSeriesAggregationBuilder("ts").subAggregation(
            new TermsAggregationBuilder("terms").field("dim_0")
        );
        final SearchResponse aggregationResponse = client().prepareSearch("index").addAggregation(timeSeries).setSize(0).get();
        final InternalTimeSeries ts = (InternalTimeSeries) aggregationResponse.getAggregations().asList().get(0);
        assertTimeSeriesAggregation(ts);
    }

    public void testDateHistogramByTsid() {
        final TimeSeriesAggregationBuilder timeSeries = new TimeSeriesAggregationBuilder("ts").subAggregation(
            new DateHistogramAggregationBuilder("date_histogram").field("@timestamp").calendarInterval(DateHistogramInterval.MINUTE)
        );
        final SearchResponse aggregationResponse = client().prepareSearch("index").addAggregation(timeSeries).setSize(0).get();
        final InternalTimeSeries ts = (InternalTimeSeries) aggregationResponse.getAggregations().asList().get(0);
        assertTimeSeriesAggregation(ts);
    }

    public void testCardinalityByTsid() {
        final TimeSeriesAggregationBuilder timeSeries = new TimeSeriesAggregationBuilder("ts").subAggregation(
            new CardinalityAggregationBuilder("cardinality").field("dim_" + (numberOfDimensions - 1))
        );
        final SearchResponse aggregationResponse = client().prepareSearch("index").addAggregation(timeSeries).setSize(0).get();
        final InternalTimeSeries ts = (InternalTimeSeries) aggregationResponse.getAggregations().asList().get(0);
        assertTimeSeriesAggregation(ts);
        assertTrue(
            ts.getBuckets()
                .stream()
                .map(bucket -> (InternalCardinality) bucket.getAggregations().get("cardinality"))
                .map(InternalCardinality::getValue)
                .allMatch(value -> value.equals(1L))
        );
    }

    private static void assertTimeSeriesAggregation(final InternalTimeSeries timeSeriesAggregation) {
        final List<Map<String, Object>> dimensions = timeSeriesAggregation.getBuckets()
            .stream()
            .map(InternalTimeSeries.InternalBucket::getKey)
            .toList();
        // NOTE: only two time series expected as a result of having just two distinct values for the last dimension
        assertEquals(2, dimensions.size());

        final Map<String, Object> firstTimeSeries = dimensions.get(0);
        final Map<String, Object> secondTimeSeries = dimensions.get(1);

        assertTsid(firstTimeSeries);
        assertTsid(secondTimeSeries);
    }

    private static void assertTsid(final Map<String, Object> timeSeries) {
        final Map.Entry<String, Object> tsidEntry = timeSeries.entrySet().stream().toList().get(0);
        assertEquals(TimeSeriesIdFieldMapper.NAME, tsidEntry.getKey());
    }
}
