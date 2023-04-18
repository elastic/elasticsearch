/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class AggregationDataExtractorFactoryTests extends ESTestCase {

    private Client client;
    private DatafeedTimingStatsReporter timingStatsReporter;

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testNewExtractor_GivenAlignedTimes() {
        AggregationDataExtractorFactory factory = createFactory(1000L);

        AggregationDataExtractor dataExtractor = (AggregationDataExtractor) factory.newExtractor(2000, 5000);

        assertThat(dataExtractor.getContext().start, equalTo(2000L));
        assertThat(dataExtractor.getContext().end, equalTo(5000L));
    }

    public void testNewExtractor_GivenNonAlignedTimes() {
        AggregationDataExtractorFactory factory = createFactory(1000L);

        AggregationDataExtractor dataExtractor = (AggregationDataExtractor) factory.newExtractor(3980, 9200);

        assertThat(dataExtractor.getContext().start, equalTo(4000L));
        assertThat(dataExtractor.getContext().end, equalTo(9000L));
    }

    private AggregationDataExtractorFactory createFactory(long histogramInterval) {
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder().addAggregator(
            AggregationBuilders.histogram("time")
                .field("time")
                .interval(histogramInterval)
                .subAggregation(AggregationBuilders.max("time").field("time"))
        );
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Detector.Builder detectorBuilder = new Detector.Builder();
        detectorBuilder.setFunction("sum");
        detectorBuilder.setFieldName("value");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detectorBuilder.build()));
        analysisConfig.setSummaryCountFieldName("doc_count");
        Job.Builder jobBuilder = new Job.Builder("foo");
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setAnalysisConfig(analysisConfig);
        DatafeedConfig.Builder datafeedConfigBuilder = new DatafeedConfig.Builder("foo-feed", jobBuilder.getId());
        datafeedConfigBuilder.setParsedAggregations(aggs);
        datafeedConfigBuilder.setIndices(Arrays.asList("my_index"));
        return new AggregationDataExtractorFactory(
            client,
            datafeedConfigBuilder.build(),
            jobBuilder.build(new Date()),
            xContentRegistry(),
            timingStatsReporter
        );
    }
}
