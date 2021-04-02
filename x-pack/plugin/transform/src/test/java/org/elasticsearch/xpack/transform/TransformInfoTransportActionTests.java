/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.transform.TransformInfoTransportAction.PROVIDED_STATS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformInfoTransportActionTests extends ESTestCase {

    public void testAvailable() {
        TransformInfoTransportAction featureSet = new TransformInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class)
        );
        assertThat(featureSet.available(), is(true));
    }

    public void testEnabledDefault() {
        TransformInfoTransportAction featureSet = new TransformInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class)
        );
        assertTrue(featureSet.enabled());
    }

    public void testParseSearchAggs() {
        Aggregations emptyAggs = new Aggregations(Collections.emptyList());
        SearchResponse withEmptyAggs = mock(SearchResponse.class);
        when(withEmptyAggs.getAggregations()).thenReturn(emptyAggs);

        assertThat(TransformInfoTransportAction.parseSearchAggs(withEmptyAggs), equalTo(new TransformIndexerStats()));

        TransformIndexerStats expectedStats = new TransformIndexerStats(
            1,  // numPages
            2,  // numInputDocuments
            3,  // numOutputDocuments
            4,  // numDeletedDocuments
            5,  // numInvocations
            6,  // indexTime
            7,  // searchTime
            8,  // processingTime
            9,  // deleteTime
            10,  // indexTotal
            11,  // searchTotal
            12, // processingTotal
            13, // indexFailures
            14, // searchFailures
            15.0,  // exponential_avg_checkpoint_duration_ms
            16.0,  // exponential_avg_documents_indexed
            17.0   // exponential_avg_documents_processed
        );

        int currentStat = 1;
        List<Aggregation> aggs = new ArrayList<>(PROVIDED_STATS.length);
        for (String statName : PROVIDED_STATS) {
            aggs.add(buildAgg(statName, currentStat++));
        }
        Aggregations aggregations = new Aggregations(aggs);
        SearchResponse withAggs = mock(SearchResponse.class);
        when(withAggs.getAggregations()).thenReturn(aggregations);

        assertThat(TransformInfoTransportAction.parseSearchAggs(withAggs), equalTo(expectedStats));
    }

    private static Aggregation buildAgg(String name, double value) {
        NumericMetricsAggregation.SingleValue agg = mock(NumericMetricsAggregation.SingleValue.class);
        when(agg.getName()).thenReturn(name);
        when(agg.value()).thenReturn(value);
        return agg;
    }
}
