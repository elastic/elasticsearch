/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.transform.TransformInfoTransportAction.PROVIDED_STATS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformInfoTransportActionTests extends ESTestCase {
    private XPackLicenseState licenseState;

    @Before
    public void init() {
        licenseState = mock(XPackLicenseState.class);
    }

    public void testAvailable() {
        TransformInfoTransportAction featureSet = new TransformInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            licenseState
        );
        boolean available = randomBoolean();
        when(licenseState.isAllowed(XPackLicenseState.Feature.TRANSFORM)).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testEnabledDefault() {
        TransformInfoTransportAction featureSet = new TransformInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            licenseState
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
            4,  // numInvocations
            5,  // indexTime
            6,  // searchTime
            7,  // processingTime
            8,  // indexTotal
            9,  // searchTotal
            10, // processingTotal
            11, // indexFailures
            12, // searchFailures
            13.0,  // exponential_avg_checkpoint_duration_ms
            14.0,  // exponential_avg_documents_indexed
            15.0   // exponential_avg_documents_processed
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
