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

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class GetAllSampleConfigurationActionIT extends ESIntegTestCase {

    public void testGetAllSampleConfigurationsWithNoConfigurations() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        // Create some indices without sampling configurations
        int numIndices = randomIntBetween(1, 3);
        for (int i = 0; i < numIndices; i++) {
            createIndex(randomIdentifier());
        }

        // Get all configurations - should return empty map
        GetAllSampleConfigurationAction.Response response = getAllSamplingConfigurations();

        assertThat("Response should not be null", response, notNullValue());
        assertThat("Configuration map should be empty", response.getIndexToSamplingConfigMap(), is(anEmptyMap()));
    }

    public void testGetAllSampleConfigurationsWithSingleConfiguration() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Create a sampling configuration
        SamplingConfiguration config = randomSamplingConfiguration();
        putSamplingConfiguration(indexName, config);

        // Get all configurations
        GetAllSampleConfigurationAction.Response response = getAllSamplingConfigurations();

        assertThat("Response should not be null", response, notNullValue());
        Map<String, SamplingConfiguration> configMap = response.getIndexToSamplingConfigMap();
        assertThat("Configuration map should not be empty", configMap, not(anEmptyMap()));
        assertThat("Configuration map should contain the index", configMap, hasKey(indexName));
        assertThat("Configuration should match", configMap.get(indexName), equalTo(config));
    }

    public void testGetAllSampleConfigurationsWithMultipleConfigurations() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        // Create multiple indices with sampling configurations
        int numIndices = randomIntBetween(2, 5);
        Map<String, SamplingConfiguration> expectedConfigs = new HashMap<>();

        for (int i = 0; i < numIndices; i++) {
            String indexName = randomIdentifier();
            createIndex(indexName);
            SamplingConfiguration config = randomSamplingConfiguration();
            putSamplingConfiguration(indexName, config);
            expectedConfigs.put(indexName, config);
        }

        // Get all configurations
        GetAllSampleConfigurationAction.Response response = getAllSamplingConfigurations();

        assertThat("Response should not be null", response, notNullValue());
        Map<String, SamplingConfiguration> configMap = response.getIndexToSamplingConfigMap();
        assertThat("Configuration map should have correct size", configMap.size(), equalTo(expectedConfigs.size()));

        // Verify each configuration matches
        for (Map.Entry<String, SamplingConfiguration> entry : expectedConfigs.entrySet()) {
            assertThat("Configuration map should contain index: " + entry.getKey(), configMap, hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    public void testGetAllSampleConfigurationsWithMixedIndices() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        // Create some indices with configurations
        int numConfiguredIndices = randomIntBetween(1, 3);
        Map<String, SamplingConfiguration> expectedConfigs = new HashMap<>();

        for (int i = 0; i < numConfiguredIndices; i++) {
            String indexName = randomIdentifier();
            createIndex(indexName);
            SamplingConfiguration config = randomSamplingConfiguration();
            putSamplingConfiguration(indexName, config);
            expectedConfigs.put(indexName, config);
        }

        // Create some indices without configurations
        int numUnconfiguredIndices = randomIntBetween(1, 3);
        for (int i = 0; i < numUnconfiguredIndices; i++) {
            createIndex(randomIdentifier());
        }

        // Get all configurations - should only return configured indices
        GetAllSampleConfigurationAction.Response response = getAllSamplingConfigurations();

        assertThat("Response should not be null", response, notNullValue());
        Map<String, SamplingConfiguration> configMap = response.getIndexToSamplingConfigMap();
        assertThat("Configuration map should only contain configured indices", configMap.size(), equalTo(expectedConfigs.size()));

        // Verify each configuration matches
        for (Map.Entry<String, SamplingConfiguration> entry : expectedConfigs.entrySet()) {
            assertThat("Configuration map should contain index: " + entry.getKey(), configMap, hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    public void testGetAllSampleConfigurationsAfterUpdate() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        String indexName = randomIdentifier();
        createIndex(indexName);

        // Create initial configuration
        SamplingConfiguration initialConfig = randomSamplingConfiguration();
        putSamplingConfiguration(indexName, initialConfig);

        // Get all configurations - should have initial config
        GetAllSampleConfigurationAction.Response response1 = getAllSamplingConfigurations();
        assertThat("Initial configuration should match", response1.getIndexToSamplingConfigMap().get(indexName), equalTo(initialConfig));

        // Update with new configuration
        SamplingConfiguration updatedConfig = randomSamplingConfiguration();
        putSamplingConfiguration(indexName, updatedConfig);

        // Get all configurations - should have updated config
        GetAllSampleConfigurationAction.Response response2 = getAllSamplingConfigurations();
        assertThat("Updated configuration should match", response2.getIndexToSamplingConfigMap().get(indexName), equalTo(updatedConfig));
    }

    public void testGetAllSampleConfigurationsAfterDeletion() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        // Create two indices with configurations
        String index1 = randomIdentifier();
        String index2 = randomIdentifier();
        createIndex(index1);
        createIndex(index2);

        SamplingConfiguration config1 = randomSamplingConfiguration();
        SamplingConfiguration config2 = randomSamplingConfiguration();
        putSamplingConfiguration(index1, config1);
        putSamplingConfiguration(index2, config2);

        // Get all configurations - should have both
        GetAllSampleConfigurationAction.Response response1 = getAllSamplingConfigurations();
        assertThat("Should have two configurations", response1.getIndexToSamplingConfigMap().size(), equalTo(2));

        // Delete one configuration
        deleteSamplingConfiguration(index1);

        // Get all configurations - should only have one
        GetAllSampleConfigurationAction.Response response2 = getAllSamplingConfigurations();
        Map<String, SamplingConfiguration> configMap = response2.getIndexToSamplingConfigMap();
        assertThat("Should have one configuration", configMap.size(), equalTo(1));
        assertThat("Should not contain deleted index", configMap, not(hasKey(index1)));
        assertThat("Should contain remaining index", configMap, hasEntry(index2, config2));
    }

    public void testGetAllSampleConfigurationsPersistsAcrossClusterStateUpdates() {
        assumeTrue("Requires the sampling feature flag to be enabled", SamplingService.RANDOM_SAMPLING_FEATURE_FLAG);

        // Create indices with sampling configurations
        int numIndices = randomIntBetween(2, 4);
        Map<String, SamplingConfiguration> expectedConfigs = new HashMap<>();

        for (int i = 0; i < numIndices; i++) {
            String indexName = randomIdentifier();
            createIndex(indexName);
            SamplingConfiguration config = randomSamplingConfiguration();
            putSamplingConfiguration(indexName, config);
            expectedConfigs.put(indexName, config);
        }

        // Get initial configurations
        GetAllSampleConfigurationAction.Response response1 = getAllSamplingConfigurations();
        assertThat("Initial configurations should match", response1.getIndexToSamplingConfigMap(), equalTo(expectedConfigs));

        // Trigger cluster state updates by creating additional indices
        int numDummyIndices = randomIntBetween(2, 5);
        for (int i = 0; i < numDummyIndices; i++) {
            createIndex(randomIdentifier());
        }

        // Get configurations again after cluster state changes
        GetAllSampleConfigurationAction.Response response2 = getAllSamplingConfigurations();
        assertThat(
            "Configurations should persist after cluster state changes",
            response2.getIndexToSamplingConfigMap(),
            equalTo(expectedConfigs)
        );
    }

    private SamplingConfiguration randomSamplingConfiguration() {
        return new SamplingConfiguration(
            randomDoubleBetween(0.1, 1.0, true),
            randomBoolean() ? randomIntBetween(1, SamplingConfiguration.MAX_SAMPLES_LIMIT) : null,
            randomBoolean() ? ByteSizeValue.ofMb(randomLongBetween(1, 100)) : null,
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

    private void deleteSamplingConfiguration(String indexName) {
        DeleteSampleConfigurationAction.Request deleteRequest = new DeleteSampleConfigurationAction.Request(
            randomValidTimeValue(),
            randomValidTimeValue()
        );
        deleteRequest.indices(indexName);
        client().execute(DeleteSampleConfigurationAction.INSTANCE, deleteRequest).actionGet();
        ensureGreen();
    }

    private GetAllSampleConfigurationAction.Response getAllSamplingConfigurations() {
        GetAllSampleConfigurationAction.Request request = new GetAllSampleConfigurationAction.Request(randomValidTimeValue());
        return client().execute(GetAllSampleConfigurationAction.INSTANCE, request).actionGet();
    }

    private TimeValue randomValidTimeValue() {
        return TimeValue.timeValueDays(randomIntBetween(10, 20));
    }
}
