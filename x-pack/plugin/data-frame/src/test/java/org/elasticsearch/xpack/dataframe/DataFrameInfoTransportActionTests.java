/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.dataframe.DataFrameInfoTransportAction.PROVIDED_STATS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataFrameInfoTransportActionTests extends ESTestCase {
    private XPackLicenseState licenseState;

    @Before
    public void init() {
        licenseState = mock(XPackLicenseState.class);
    }

    public void testAvailable() {
        DataFrameInfoTransportAction featureSet = new DataFrameInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), Settings.EMPTY, licenseState);
        boolean available = randomBoolean();
        when(licenseState.isDataFrameAllowed()).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testEnabledSetting() {
        boolean enabled = randomBoolean();
        Settings.Builder settings = Settings.builder();
        settings.put("xpack.data_frame.enabled", enabled);
        DataFrameInfoTransportAction featureSet = new DataFrameInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), settings.build(), licenseState);
        assertThat(featureSet.enabled(), is(enabled));
    }

    public void testEnabledDefault() {
        DataFrameInfoTransportAction featureSet = new DataFrameInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), Settings.EMPTY, licenseState);
        assertTrue(featureSet.enabled());
    }

    public void testParseSearchAggs() {
        Aggregations emptyAggs = new Aggregations(Collections.emptyList());
        SearchResponse withEmptyAggs = mock(SearchResponse.class);
        when(withEmptyAggs.getAggregations()).thenReturn(emptyAggs);

        assertThat(DataFrameInfoTransportAction.parseSearchAggs(withEmptyAggs),
            equalTo(new DataFrameIndexerTransformStats()));

        DataFrameIndexerTransformStats expectedStats = new DataFrameIndexerTransformStats(
            1,  // numPages
            2,  // numInputDocuments
            3,  // numOutputDocuments
            4,  // numInvocations
            5,  // indexTime
            6,  // searchTime
            7,  // indexTotal
            8,  // searchTotal
            9,  // indexFailures
            10); // searchFailures

        int currentStat = 1;
        List<Aggregation> aggs = new ArrayList<>(PROVIDED_STATS.length);
        for (String statName : PROVIDED_STATS) {
            aggs.add(buildAgg(statName, (double) currentStat++));
        }
        Aggregations aggregations = new Aggregations(aggs);
        SearchResponse withAggs = mock(SearchResponse.class);
        when(withAggs.getAggregations()).thenReturn(aggregations);

        assertThat(DataFrameInfoTransportAction.parseSearchAggs(withAggs), equalTo(expectedStats));
    }

    private static Aggregation buildAgg(String name, double value) {
        NumericMetricsAggregation.SingleValue agg = mock(NumericMetricsAggregation.SingleValue.class);
        when(agg.getName()).thenReturn(name);
        when(agg.value()).thenReturn(value);
        return agg;
    }

    public void testUsageDisabled() throws IOException, InterruptedException, ExecutionException {
        when(licenseState.isDataFrameAllowed()).thenReturn(true);
        Settings.Builder settings = Settings.builder();
        settings.put("xpack.data_frame.enabled", false);
        var usageAction = new DataFrameUsageTransportAction(mock(TransportService.class), null, null,
            mock(ActionFilters.class), null, settings.build(), licenseState, mock(Client.class));
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, mock(ClusterState.class), future);
        XPackFeatureSet.Usage usage = future.get().getUsage();

        assertFalse(usage.enabled());
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);

            XContentParser parser = createParser(builder);
            Map<String, Object> usageAsMap = parser.map();
            assertTrue((boolean) XContentMapValues.extractValue("available", usageAsMap));
            assertFalse((boolean) XContentMapValues.extractValue("enabled", usageAsMap));
            // not enabled -> no transforms, no stats
            assertEquals(null, XContentMapValues.extractValue("transforms", usageAsMap));
            assertEquals(null, XContentMapValues.extractValue("stats", usageAsMap));
        }
    }
}
