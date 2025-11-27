/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class GetSampleActionIT extends ESIntegTestCase {

    public void testGetSample() throws Exception {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);
        String indexName = randomIdentifier();
        // the index doesn't exist, so getting its sample will throw an exception:
        assertGetSampleThrowsResourceNotFoundException(indexName);
        createIndex(indexName);
        // the index exists but there is no sampling configuration for it, so getting its sample will throw an exception:
        assertGetSampleThrowsResourceNotFoundException(indexName);
        final int maxSamples = 30;
        addSamplingConfig(indexName, maxSamples);
        // There is now a sampling configuration, but no data has been ingested:
        assertEmptySample(indexName);
        int docsToIndex = randomIntBetween(1, 20);
        for (int i = 0; i < docsToIndex; i++) {
            indexDoc(indexName, randomIdentifier(), randomAlphanumericOfLength(10), randomAlphanumericOfLength(10));
        }
        GetSampleAction.Request request = new GetSampleAction.Request(indexName);
        GetSampleAction.Response response = client().execute(GetSampleAction.INSTANCE, request).actionGet();
        List<SamplingService.RawDocument> sample = response.getSample();
        // The sampling config created by addSamplingConfig samples at 100%, so we expect everything to be sampled:
        assertThat(sample.size(), equalTo(docsToIndex));
        for (int i = 0; i < docsToIndex; i++) {
            assertRawDocument(sample.get(i), indexName);
        }

        GetSampleStatsAction.Request statsRequest = new GetSampleStatsAction.Request(indexName);
        GetSampleStatsAction.Response statsResponse = client().execute(GetSampleStatsAction.INSTANCE, statsRequest).actionGet();
        SamplingService.SampleStats stats = statsResponse.getSampleStats();
        assertThat(stats.getSamples(), equalTo((long) docsToIndex));
        assertThat(stats.getPotentialSamples(), equalTo((long) docsToIndex));
        assertThat(stats.getTimeSampling(), greaterThan(TimeValue.ZERO));
        assertThat(stats.getSamplesRejectedForMaxSamplesExceeded(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForRate(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForCondition(), equalTo(0L));
        assertThat(stats.getSamplesRejectedForCondition(), equalTo(0L));

        final int samplesOverMax = randomIntBetween(1, 5);
        for (int i = docsToIndex; i < maxSamples + samplesOverMax; i++) {
            indexDoc(indexName, randomIdentifier(), randomAlphanumericOfLength(10), randomAlphanumericOfLength(10));
        }
        statsRequest = new GetSampleStatsAction.Request(indexName);
        statsResponse = client().execute(GetSampleStatsAction.INSTANCE, statsRequest).actionGet();
        stats = statsResponse.getSampleStats();
        assertThat(stats.getSamples(), equalTo((long) maxSamples));
        assertThat(stats.getPotentialSamples(), equalTo((long) maxSamples + samplesOverMax));
        assertThat(stats.getSamplesRejectedForMaxSamplesExceeded(), equalTo((long) samplesOverMax));

    }

    private void assertRawDocument(SamplingService.RawDocument rawDocument, String indexName) {
        assertThat(rawDocument.indexName(), equalTo(indexName));
    }

    private void assertEmptySample(String indexName) {
        GetSampleAction.Request request = new GetSampleAction.Request(indexName);
        GetSampleAction.Response response = client().execute(GetSampleAction.INSTANCE, request).actionGet();
        List<SamplingService.RawDocument> sample = response.getSample();
        assertThat(sample, equalTo(List.of()));
    }

    private void assertGetSampleThrowsResourceNotFoundException(String indexName) {
        GetSampleAction.Request request = new GetSampleAction.Request(indexName);
        assertThrows(ResourceNotFoundException.class, () -> client().execute(GetSampleAction.INSTANCE, request).actionGet());
    }

    private void addSamplingConfig(String indexName, int maxSamples) throws Exception {
        SamplingConfiguration samplingConfiguration = new SamplingConfiguration(1.0d, maxSamples, null, null, null);
        client().execute(
            PutSampleConfigurationAction.INSTANCE,
            new PutSampleConfigurationAction.Request(samplingConfiguration, TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).indices(
                indexName
            )
        ).actionGet();
    }
}
