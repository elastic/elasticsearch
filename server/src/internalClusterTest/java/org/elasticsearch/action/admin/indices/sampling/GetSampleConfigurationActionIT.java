/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetSampleConfigurationActionIT extends ESIntegTestCase {

    public void testGetSampleConfiguration() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Create a random sampling configuration
        SamplingConfiguration config = randomSamplingConfiguration();
        putSamplingConfiguration(indexName, config);

        // Get and verify the sampling configuration
        assertGetConfigurationMatches(indexName, config);
    }

    public void testGetSampleConfigurationForNonExistentIndex() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String nonExistentIndex = randomIdentifier();

        // Try to get configuration for non-existent index
        GetSampleConfigurationAction.Request getRequest = new GetSampleConfigurationAction.Request(randomValidTimeValue());
        getRequest.indices(nonExistentIndex);

        // This should fail - cannot get config for non-existent index
        expectThrows(Exception.class, () -> client().execute(GetSampleConfigurationAction.INSTANCE, getRequest).actionGet());
    }

    public void testGetSampleConfigurationForIndexWithoutConfiguration() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Get configuration for index without any sampling configuration
        GetSampleConfigurationAction.Request getRequest = new GetSampleConfigurationAction.Request(randomValidTimeValue());
        getRequest.indices(indexName);

        GetSampleConfigurationAction.Response response = client().execute(GetSampleConfigurationAction.INSTANCE, getRequest).actionGet();

        // Verify response returns null configuration
        assertThat("Response should not be null", response, notNullValue());
        assertThat("Index name should match", response.getIndex(), equalTo(indexName));
        assertThat("Configuration should be null for index without config", response.getConfiguration(), nullValue());
    }

    public void testGetSampleConfigurationAfterUpdate() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Create initial random configuration
        SamplingConfiguration initialConfig = randomSamplingConfiguration();
        putSamplingConfiguration(indexName, initialConfig);

        // Get initial configuration
        assertGetConfigurationMatches(indexName, initialConfig);

        // Update with new random configuration
        SamplingConfiguration updatedConfig = randomSamplingConfiguration();
        putSamplingConfiguration(indexName, updatedConfig);

        // Get and verify updated configuration
        assertGetConfigurationMatches(indexName, updatedConfig);
    }

    public void testGetSampleConfigurationPersistsAcrossClusterStateUpdates() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Store random sampling configuration
        SamplingConfiguration config = randomSamplingConfiguration();
        putSamplingConfiguration(indexName, config);

        // Get initial configuration
        assertGetConfigurationMatches(indexName, config);

        // Trigger cluster state updates by creating additional indices with random names
        int numDummyIndices = randomIntBetween(2, 5);
        for (int i = 0; i < numDummyIndices; i++) {
            createIndex(randomIdentifier());
        }

        // Get configuration again after cluster state changes and verify it persists
        assertGetConfigurationMatches(indexName, config);
    }

    private SamplingConfiguration randomSamplingConfiguration() {
        return new SamplingConfiguration(
            randomDoubleBetween(0.1, 1.0, true),
            randomBoolean() ? randomIntBetween(1, SamplingConfiguration.MAX_SAMPLES_LIMIT) : null,
            randomBoolean() ? ByteSizeValue.ofMb(randomIntBetween(1, 100)) : null,
            randomBoolean() ? randomValidTimeValue() : null,
            randomBoolean() ? randomAlphaOfLengthBetween(5, 30) : null
        );
    }

    private void putSamplingConfiguration(String indexName, SamplingConfiguration config) {
        PutSampleConfigurationAction.Request putRequest = new PutSampleConfigurationAction.Request(
            config,
            randomValidTimeValue(),
            randomValidTimeValue()
        );
        putRequest.indices(indexName);
        client().execute(PutSampleConfigurationAction.INSTANCE, putRequest).actionGet();
        ensureGreen();
    }

    private void assertGetConfigurationMatches(String indexName, SamplingConfiguration expectedConfig) {
        GetSampleConfigurationAction.Request getRequest = new GetSampleConfigurationAction.Request(randomValidTimeValue());
        getRequest.indices(indexName);

        GetSampleConfigurationAction.Response response = client().execute(GetSampleConfigurationAction.INSTANCE, getRequest).actionGet();

        assertThat("Response should not be null", response, notNullValue());
        assertThat("Index name should match", response.getIndex(), equalTo(indexName));
        assertThat("Configuration should match", response.getConfiguration(), equalTo(expectedConfig));
    }

    private TimeValue randomValidTimeValue() {
        return TimeValue.timeValueDays(randomIntBetween(10, 20));
    }
}
