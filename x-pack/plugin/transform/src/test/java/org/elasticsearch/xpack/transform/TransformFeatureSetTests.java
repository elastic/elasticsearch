/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
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
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackFeatureSet.Usage;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.transform.TransformFeatureSet.PROVIDED_STATS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformFeatureSetTests extends ESTestCase {
    private XPackLicenseState licenseState;

    @Before
    public void init() {
        licenseState = mock(XPackLicenseState.class);
    }

    public void testAvailable() {
        TransformFeatureSet featureSet = new TransformFeatureSet(
            Settings.EMPTY,
            mock(ClusterService.class),
            mock(Client.class),
            licenseState
        );
        boolean available = randomBoolean();
        when(licenseState.isTransformAllowed()).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testEnabledSetting() {
        boolean enabled = randomBoolean();
        Settings.Builder settings = Settings.builder();
        settings.put("xpack.transform.enabled", enabled);
        TransformFeatureSet featureSet = new TransformFeatureSet(
            settings.build(),
            mock(ClusterService.class),
            mock(Client.class),
            licenseState
        );
        assertThat(featureSet.enabled(), is(enabled));
    }

    public void testEnabledSettingFallback() {
        boolean enabled = randomBoolean();
        Settings.Builder settings = Settings.builder();
        // use the deprecated setting
        settings.put("xpack.data_frame.enabled", enabled);
        TransformFeatureSet featureSet = new TransformFeatureSet(
            settings.build(),
            mock(ClusterService.class),
            mock(Client.class),
            licenseState
        );
        assertThat(featureSet.enabled(), is(enabled));
        assertWarnings(
            "[xpack.data_frame.enabled] setting was deprecated in Elasticsearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testEnabledSettingFallbackMix() {
        Settings.Builder settings = Settings.builder();
        // use the deprecated setting
        settings.put("xpack.data_frame.enabled", false);
        settings.put("xpack.transform.enabled", true);
        TransformFeatureSet featureSet = new TransformFeatureSet(
            settings.build(),
            mock(ClusterService.class),
            mock(Client.class),
            licenseState
        );
        assertThat(featureSet.enabled(), is(true));
        assertWarnings(
            "[xpack.data_frame.enabled] setting was deprecated in Elasticsearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testEnabledDefault() {
        TransformFeatureSet featureSet = new TransformFeatureSet(
            Settings.EMPTY,
            mock(ClusterService.class),
            mock(Client.class),
            licenseState
        );
        assertTrue(featureSet.enabled());
    }

    public void testParseSearchAggs() {
        Aggregations emptyAggs = new Aggregations(Collections.emptyList());
        SearchResponse withEmptyAggs = mock(SearchResponse.class);
        when(withEmptyAggs.getAggregations()).thenReturn(emptyAggs);

        assertThat(TransformFeatureSet.parseSearchAggs(withEmptyAggs), equalTo(new TransformIndexerStats()));

        TransformIndexerStats expectedStats = new TransformIndexerStats(
            1,  // numPages
            2,  // numInputDocuments
            3,  // numOutputDocuments
            4,  // numInvocations
            5,  // indexTime
            6,  // searchTime
            7,  // indexTotal
            8,  // searchTotal
            9,  // indexFailures
            10, // searchFailures
            11.0,  // exponential_avg_checkpoint_duration_ms
            12.0,  // exponential_avg_documents_indexed
            13.0   // exponential_avg_documents_processed
        );

        int currentStat = 1;
        List<Aggregation> aggs = new ArrayList<>(PROVIDED_STATS.length);
        for (String statName : PROVIDED_STATS) {
            aggs.add(buildAgg(statName, currentStat++));
        }
        Aggregations aggregations = new Aggregations(aggs);
        SearchResponse withAggs = mock(SearchResponse.class);
        when(withAggs.getAggregations()).thenReturn(aggregations);

        assertThat(TransformFeatureSet.parseSearchAggs(withAggs), equalTo(expectedStats));
    }

    private static Aggregation buildAgg(String name, double value) {
        NumericMetricsAggregation.SingleValue agg = mock(NumericMetricsAggregation.SingleValue.class);
        when(agg.getName()).thenReturn(name);
        when(agg.value()).thenReturn(value);
        return agg;
    }

    public void testUsageDisabled() throws IOException, InterruptedException, ExecutionException {
        when(licenseState.isTransformAllowed()).thenReturn(true);
        Settings.Builder settings = Settings.builder();
        settings.put("xpack.transform.enabled", false);
        TransformFeatureSet featureSet = new TransformFeatureSet(
            settings.build(),
            mock(ClusterService.class),
            mock(Client.class),
            licenseState
        );
        PlainActionFuture<Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();

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
