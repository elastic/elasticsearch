/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

abstract class AbstractElasticsearchInternalServiceSettingsTests<T extends ElasticsearchInternalServiceSettings> extends
    AbstractWireSerializingTestCase<T> {

    protected abstract void assertUpdated(T original, T updated);

    @SuppressWarnings("unchecked")
    public void testUpdateNumAllocations() {
        var testInstance = createTestInstance();
        var expectedNumAllocations = testInstance.getNumAllocations() != null ? testInstance.getNumAllocations() + 1 : 1;
        var updatedInstance = testInstance.updateServiceSettings(
            Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, expectedNumAllocations)
        );

        assertThat("update should create a new instance", updatedInstance, not(equalTo(testInstance)));
        assertThat(updatedInstance.getClass(), equalTo(testInstance.getClass()));
        assertThat(updatedInstance, instanceOf(ElasticsearchInternalServiceSettings.class));
        var updatedElasticSearchInternalServiceSettings = (ElasticsearchInternalServiceSettings) updatedInstance;
        assertThat(updatedElasticSearchInternalServiceSettings.getNumAllocations(), equalTo(expectedNumAllocations));
        assertThat(updatedElasticSearchInternalServiceSettings.getAdaptiveAllocationsSettings(), nullValue());
        assertThat(updatedElasticSearchInternalServiceSettings.getNumThreads(), equalTo(testInstance.getNumThreads()));
        assertThat(updatedElasticSearchInternalServiceSettings.getDeploymentId(), equalTo(testInstance.getDeploymentId()));
        assertThat(updatedElasticSearchInternalServiceSettings.modelId(), equalTo(testInstance.modelId()));
        assertUpdated(testInstance, (T) updatedInstance);
    }

    @SuppressWarnings("unchecked")
    public void testUpdateAdaptiveAllocations() throws IOException {
        var testInstance = createTestInstance();
        var expectedAdaptiveAllocations = adaptiveAllocationSettings(testInstance.getAdaptiveAllocationsSettings());
        var updatedInstance = testInstance.updateServiceSettings(
            Map.of(ElasticsearchInternalServiceSettings.ADAPTIVE_ALLOCATIONS, toMap(expectedAdaptiveAllocations))
        );

        assertThat("update should create a new instance", updatedInstance, not(equalTo(testInstance)));
        assertThat(updatedInstance.getClass(), equalTo(testInstance.getClass()));
        assertThat(updatedInstance, instanceOf(ElasticsearchInternalServiceSettings.class));
        var updatedElasticSearchInternalServiceSettings = (ElasticsearchInternalServiceSettings) updatedInstance;
        assertThat(updatedElasticSearchInternalServiceSettings.getNumAllocations(), nullValue());
        assertThat(updatedElasticSearchInternalServiceSettings.getAdaptiveAllocationsSettings(), equalTo(expectedAdaptiveAllocations));
        assertThat(updatedElasticSearchInternalServiceSettings.getNumThreads(), equalTo(testInstance.getNumThreads()));
        assertThat(updatedElasticSearchInternalServiceSettings.getDeploymentId(), equalTo(testInstance.getDeploymentId()));
        assertThat(updatedElasticSearchInternalServiceSettings.modelId(), equalTo(testInstance.modelId()));
        assertUpdated(testInstance, (T) updatedInstance);
    }

    public void testUpdateNumAllocationsAndAdaptiveAllocations() {
        var validationException = assertThrows(ValidationException.class, () -> {
            createTestInstance().updateServiceSettings(
                Map.ofEntries(
                    Map.entry(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1),
                    Map.entry(ElasticsearchInternalServiceSettings.ADAPTIVE_ALLOCATIONS, toMap(adaptiveAllocationSettings(null)))
                )
            );
        });
        assertThat(
            validationException.getMessage(),
            equalTo("Validation Failed: 1: [num_allocations] cannot be set if [adaptive_allocations] is set;")
        );
    }

    public void testUpdateWithNoNumAllocationsAndAdaptiveAllocations() {
        var validationException = assertThrows(ValidationException.class, () -> createTestInstance().updateServiceSettings(Map.of()));
        assertThat(validationException.getMessage(), equalTo("""
            Validation Failed: 1: [service_settings] does not contain one of the required settings \
            [num_allocations, adaptive_allocations];"""));
    }

    private static AdaptiveAllocationsSettings adaptiveAllocationSettings(AdaptiveAllocationsSettings base) {
        if (base == null) {
            base = new AdaptiveAllocationsSettings(true, 0, 1);
        } else {
            base = new AdaptiveAllocationsSettings(true, base.getMinNumberOfAllocations() + 1, base.getMaxNumberOfAllocations() + 1);
        }
        return base;
    }

    private static Map<String, ?> toMap(AdaptiveAllocationsSettings adaptiveAllocationsSettings) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            adaptiveAllocationsSettings.toXContent(builder, ToXContent.EMPTY_PARAMS);
            var bytes = Strings.toString(builder).getBytes(StandardCharsets.UTF_8);
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, bytes)) {
                return parser.map();
            }
        }
    }
}
