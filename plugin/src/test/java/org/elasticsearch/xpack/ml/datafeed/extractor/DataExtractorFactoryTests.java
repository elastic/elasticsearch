/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunnerTests;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.chunked.ChunkedDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.scroll.ScrollDataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.junit.Before;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class DataExtractorFactoryTests extends ESTestCase {

    private Client client;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
    }

    public void testCreateDataExtractorFactoryGivenDefaultScroll() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedJobRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        DatafeedConfig datafeedConfig = DatafeedJobRunnerTests.createDatafeedConfig("datafeed1", "foo").build();

        DataExtractorFactory dataExtractorFactory = DataExtractorFactory.create(client, datafeedConfig, jobBuilder.build());

        assertThat(dataExtractorFactory, instanceOf(ChunkedDataExtractorFactory.class));
    }

    public void testCreateDataExtractorFactoryGivenScrollWithAutoChunk() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedJobRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        DatafeedConfig.Builder datafeedConfig = DatafeedJobRunnerTests.createDatafeedConfig("datafeed1", "foo");
        datafeedConfig.setChunkingConfig(ChunkingConfig.newAuto());

        DataExtractorFactory dataExtractorFactory = DataExtractorFactory.create(client, datafeedConfig.build(), jobBuilder.build());

        assertThat(dataExtractorFactory, instanceOf(ChunkedDataExtractorFactory.class));
    }

    public void testCreateDataExtractorFactoryGivenScrollWithOffChunk() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedJobRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        DatafeedConfig.Builder datafeedConfig = DatafeedJobRunnerTests.createDatafeedConfig("datafeed1", "foo");
        datafeedConfig.setChunkingConfig(ChunkingConfig.newOff());

        DataExtractorFactory dataExtractorFactory = DataExtractorFactory.create(client, datafeedConfig.build(), jobBuilder.build());

        assertThat(dataExtractorFactory, instanceOf(ScrollDataExtractorFactory.class));
    }

    public void testCreateDataExtractorFactoryGivenAggregation() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedJobRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        DatafeedConfig.Builder datafeedConfig = DatafeedJobRunnerTests.createDatafeedConfig("datafeed1", "foo");
        datafeedConfig.setAggregations(AggregatorFactories.builder().addAggregator(AggregationBuilders.avg("a")));

        DataExtractorFactory dataExtractorFactory = DataExtractorFactory.create(client, datafeedConfig.build(), jobBuilder.build());

        assertThat(dataExtractorFactory, instanceOf(AggregationDataExtractorFactory.class));
    }

    public void testCreateDataExtractorFactoryGivenDefaultAggregationWithAutoChunk() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedJobRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        DatafeedConfig.Builder datafeedConfig = DatafeedJobRunnerTests.createDatafeedConfig("datafeed1", "foo");
        datafeedConfig.setAggregations(AggregatorFactories.builder());
        datafeedConfig.setChunkingConfig(ChunkingConfig.newAuto());

        DataExtractorFactory dataExtractorFactory = DataExtractorFactory.create(client, datafeedConfig.build(), jobBuilder.build());

        assertThat(dataExtractorFactory, instanceOf(ChunkedDataExtractorFactory.class));
    }
}