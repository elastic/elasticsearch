/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

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
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class ChunkedDataExtractorFactoryTests extends ESTestCase {

    private Client client;
    private DataExtractorFactory dataExtractorFactory;
    private DatafeedTimingStatsReporter timingStatsReporter;

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        dataExtractorFactory = mock(DataExtractorFactory.class);
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
    }

    public void testNewExtractor_GivenAlignedTimes() {
        ChunkedDataExtractorFactory factory = createFactory(1000L);

        ChunkedDataExtractor dataExtractor = (ChunkedDataExtractor) factory.newExtractor(2000, 5000);

        assertThat(dataExtractor.getContext().start, equalTo(2000L));
        assertThat(dataExtractor.getContext().end, equalTo(5000L));
    }

    public void testNewExtractor_GivenNonAlignedTimes() {
        ChunkedDataExtractorFactory factory = createFactory(1000L);

        ChunkedDataExtractor dataExtractor = (ChunkedDataExtractor) factory.newExtractor(3980, 9200);

        assertThat(dataExtractor.getContext().start, equalTo(4000L));
        assertThat(dataExtractor.getContext().end, equalTo(9000L));
    }

    public void testIntervalTimeAligner() {
        ChunkedDataExtractorContext.TimeAligner timeAligner = ChunkedDataExtractorFactory.newIntervalTimeAligner(100L);
        assertThat(timeAligner.alignToFloor(300L), equalTo(300L));
        assertThat(timeAligner.alignToFloor(301L), equalTo(300L));
        assertThat(timeAligner.alignToFloor(399L), equalTo(300L));
        assertThat(timeAligner.alignToFloor(400L), equalTo(400L));
        assertThat(timeAligner.alignToCeil(300L), equalTo(300L));
        assertThat(timeAligner.alignToCeil(301L), equalTo(400L));
        assertThat(timeAligner.alignToCeil(399L), equalTo(400L));
        assertThat(timeAligner.alignToCeil(400L), equalTo(400L));
    }

    public void testIdentityTimeAligner() {
        ChunkedDataExtractorContext.TimeAligner timeAligner = ChunkedDataExtractorFactory.newIdentityTimeAligner();
        assertThat(timeAligner.alignToFloor(300L), equalTo(300L));
        assertThat(timeAligner.alignToFloor(301L), equalTo(301L));
        assertThat(timeAligner.alignToFloor(399L), equalTo(399L));
        assertThat(timeAligner.alignToFloor(400L), equalTo(400L));
        assertThat(timeAligner.alignToCeil(300L), equalTo(300L));
        assertThat(timeAligner.alignToCeil(301L), equalTo(301L));
        assertThat(timeAligner.alignToCeil(399L), equalTo(399L));
        assertThat(timeAligner.alignToCeil(400L), equalTo(400L));
    }

    private ChunkedDataExtractorFactory createFactory(long histogramInterval) {
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
        return new ChunkedDataExtractorFactory(
            client,
            datafeedConfigBuilder.build(),
            jobBuilder.build(new Date()),
            xContentRegistry(),
            dataExtractorFactory,
            timingStatsReporter
        );
    }
}
