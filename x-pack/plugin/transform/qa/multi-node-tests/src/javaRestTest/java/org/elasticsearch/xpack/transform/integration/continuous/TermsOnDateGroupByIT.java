/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test case for `terms` on a date type field
 *
 * Note: dates are currently written as long (epoch_ms) into the transform dest index.
 */
public class TermsOnDateGroupByIT extends ContinuousTestCase {

    private static final String NAME = "continuous-terms-on-date-pivot-test";
    private static final String MISSING_BUCKET_KEY = ContinuousTestCase.STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS.withZone(ZoneId.of("UTC"))
        .format(Instant.ofEpochMilli(1262304000000L)); // 01/01/2010 should end up last when sorting

    private final boolean missing;
    private final String metricField;
    private final String timestampField;

    public TermsOnDateGroupByIT() {
        missing = randomBoolean();
        metricField = randomFrom(METRIC_FIELDS);
        timestampField = randomFrom(OTHER_TIMESTAMP_FIELDS);
    }

    @Override
    public TransformConfig createConfig() {
        TransformConfig.Builder transformConfigBuilder = new TransformConfig.Builder();
        addCommonBuilderParameters(transformConfigBuilder);
        transformConfigBuilder.setSource(new SourceConfig(CONTINUOUS_EVENTS_SOURCE_INDEX));
        transformConfigBuilder.setDest(new DestConfig(NAME, INGEST_PIPELINE));
        transformConfigBuilder.setId(NAME);

        PivotConfig.Builder pivotConfigBuilder = new PivotConfig.Builder();
        pivotConfigBuilder.setGroups(
            new GroupConfig.Builder().groupBy(
                "some-timestamp",
                new TermsGroupSource.Builder().setField(timestampField).setMissingBucket(missing).build()
            ).build()
        );

        AggregatorFactories.Builder aggregations = new AggregatorFactories.Builder();
        addCommonAggregations(aggregations);
        aggregations.addAggregator(AggregationBuilders.avg("metric.avg").field(metricField));

        pivotConfigBuilder.setAggregations(aggregations);
        transformConfigBuilder.setPivotConfig(pivotConfigBuilder.build());
        return transformConfigBuilder.build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void testIteration(int iteration, Set<String> modifiedEvents) throws IOException {
        SearchRequest searchRequestSource = new SearchRequest(CONTINUOUS_EVENTS_SOURCE_INDEX).allowPartialSearchResults(false)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        SearchSourceBuilder sourceBuilderSource = new SearchSourceBuilder().size(0);
        TermsAggregationBuilder terms = new TermsAggregationBuilder("some-timestamp").size(1000)
            .field(timestampField)
            .order(BucketOrder.key(true));
        if (missing) {
            // missing_bucket produces `null`, we can't use `null` in aggs, so we have to use a magic value, see gh#60043
            terms.missing(MISSING_BUCKET_KEY);
        }

        terms.subAggregation(AggregationBuilders.avg("metric.avg").field(metricField));
        sourceBuilderSource.aggregation(terms);
        searchRequestSource.source(sourceBuilderSource);
        SearchResponse responseSource = search(searchRequestSource);

        SearchRequest searchRequestDest = new SearchRequest(NAME).allowPartialSearchResults(false)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchSourceBuilder sourceBuilderDest = new SearchSourceBuilder().size(1000).sort("some-timestamp");
        searchRequestDest.source(sourceBuilderDest);
        SearchResponse responseDest = search(searchRequestDest);

        List<? extends Bucket> buckets = ((Terms) responseSource.getAggregations().get("some-timestamp")).getBuckets();

        // the number of search hits should be equal to the number of buckets returned by the aggregation
        assertThat(
            "Number of buckets did not match, source: "
                + responseDest.getHits().getTotalHits().value
                + ", expected: "
                + Long.valueOf(buckets.size())
                + ", iteration: "
                + iteration,
            responseDest.getHits().getTotalHits().value,
            equalTo(Long.valueOf(buckets.size()))
        );

        Iterator<? extends Bucket> sourceIterator = buckets.iterator();
        Iterator<SearchHit> destIterator = responseDest.getHits().iterator();

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            Bucket bucket = sourceIterator.next();
            SearchHit searchHit = destIterator.next();
            Map<String, Object> source = searchHit.getSourceAsMap();

            String transformBucketKey = (String) XContentMapValues.extractValue("some-timestamp", source);

            if (transformBucketKey == null) {
                transformBucketKey = MISSING_BUCKET_KEY;
            }
            // test correctness, the results from the aggregation and the results from the transform should be the same
            assertThat(
                "Buckets did not match, source: " + source + ", expected: " + bucket.getKeyAsString() + ", iteration: " + iteration,
                transformBucketKey,
                equalTo(bucket.getKeyAsString())
            );
            assertThat(
                "Doc count did not match, source: " + source + ", expected: " + bucket.getDocCount() + ", iteration: " + iteration,
                ((Integer) XContentMapValues.extractValue("count", source)).longValue(),
                equalTo(bucket.getDocCount())
            );

            SingleValue avgAgg = (SingleValue) bucket.getAggregations().get("metric.avg");
            assertThat(
                "Metric aggregation did not match, source: " + source + ", expected: " + avgAgg.value() + ", iteration: " + iteration,
                XContentMapValues.extractValue("metric.avg", source),
                equalTo(avgAgg.value())
            );

            // test optimization, transform should only rewrite documents that require it
            // run.ingest is set by the pipeline, run.max is set by the transform
            // run.ingest > run.max means, the data point has been re-created/re-fed although it wasn't necessary,
            // this is probably a bug in transform's change collection optimization
            // run.ingest < run.max means the ingest pipeline wasn't updated, this might be a bug in put pipeline
            assertThat(
                "Ingest run: "
                    + XContentMapValues.extractValue(INGEST_RUN_FIELD, source)
                    + " did not match max run: "
                    + XContentMapValues.extractValue(MAX_RUN_FIELD, source)
                    + ", iteration: "
                    + iteration
                    + " full source: "
                    + source,
                XContentMapValues.extractValue(INGEST_RUN_FIELD, source),
                equalTo(XContentMapValues.extractValue(MAX_RUN_FIELD, source))
            );
        }
        assertFalse(sourceIterator.hasNext());
        assertFalse(destIterator.hasNext());
    }
}
