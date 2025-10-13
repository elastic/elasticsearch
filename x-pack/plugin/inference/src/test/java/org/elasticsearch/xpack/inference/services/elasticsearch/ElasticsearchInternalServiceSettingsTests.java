/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ElasticsearchInternalServiceSettingsTests extends AbstractWireSerializingTestCase<ElasticsearchInternalServiceSettings> {

    public static ElasticsearchInternalServiceSettings validInstance(String modelId) {
        boolean useAdaptive = randomBoolean();
        var deploymentId = randomBoolean() ? null : randomAlphaOfLength(5);
        if (useAdaptive) {
            var adaptive = new AdaptiveAllocationsSettings(true, 1, randomIntBetween(2, 8));
            return new ElasticsearchInternalServiceSettings(
                randomBoolean() ? 1 : null,
                randomIntBetween(1, 16),
                modelId,
                adaptive,
                deploymentId
            );
        } else {
            return new ElasticsearchInternalServiceSettings(randomIntBetween(1, 10), randomIntBetween(1, 16), modelId, null, deploymentId);
        }
    }

    @Override
    protected Writeable.Reader<ElasticsearchInternalServiceSettings> instanceReader() {
        return ElasticsearchInternalServiceSettings::new;
    }

    @Override
    protected ElasticsearchInternalServiceSettings createTestInstance() {
        return validInstance("my-model");
    }

    @Override
    protected ElasticsearchInternalServiceSettings mutateInstance(ElasticsearchInternalServiceSettings instance) throws IOException {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(
                    instance.getNumAllocations() == null ? 1 : instance.getNumAllocations() + 1,
                    instance.getNumThreads(),
                    instance.modelId(),
                    instance.getAdaptiveAllocationsSettings(),
                    instance.getDeploymentId()
                )
            );
            case 1 -> new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(
                    instance.getNumAllocations(),
                    instance.getNumThreads() + 1,
                    instance.modelId(),
                    instance.getAdaptiveAllocationsSettings(),
                    instance.getDeploymentId()
                )
            );
            case 2 -> new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(
                    instance.getNumAllocations(),
                    instance.getNumThreads(),
                    instance.modelId() + "-bar",
                    instance.getAdaptiveAllocationsSettings(),
                    instance.getDeploymentId()
                )
            );
            default -> throw new IllegalStateException();
        };
    }

    public void testFromRequestMap_NoDefaultModel() {
        var serviceSettingsBuilder = ElasticsearchInternalServiceSettings.fromRequestMap(
            new HashMap<>(
                Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1, ElasticsearchInternalServiceSettings.NUM_THREADS, 4)
            )
        );
        assertNull(serviceSettingsBuilder.getModelId());
    }

    public void testFromMap() {
        var serviceSettings = ElasticsearchInternalServiceSettings.fromRequestMap(
            new HashMap<>(
                Map.of(
                    ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                    1,
                    ElasticsearchInternalServiceSettings.NUM_THREADS,
                    4,
                    ElasticsearchInternalServiceSettings.MODEL_ID,
                    ".elser_model_1"
                )
            )
        ).build();
        assertEquals(new ElasticsearchInternalServiceSettings(1, 4, ".elser_model_1", null, null), serviceSettings);
    }

    public void testFromMapMissingOptions() {
        var e = expectThrows(
            ValidationException.class,
            () -> ElasticsearchInternalServiceSettings.fromRequestMap(
                new HashMap<>(Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1))
            )
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [num_threads]"));

        e = expectThrows(
            ValidationException.class,
            () -> ElasticsearchInternalServiceSettings.fromRequestMap(
                new HashMap<>(Map.of(ElasticsearchInternalServiceSettings.NUM_THREADS, 1))
            )
        );

        assertThat(
            e.getMessage(),
            containsString("[service_settings] does not contain one of the required settings [num_allocations, adaptive_allocations]")
        );
    }

    public void testFromMapInvalidSettings() {
        var settingsMap = new HashMap<String, Object>(
            Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 0, ElasticsearchInternalServiceSettings.NUM_THREADS, -1)
        );
        var e = expectThrows(ValidationException.class, () -> ElasticsearchInternalServiceSettings.fromRequestMap(settingsMap));

        assertThat(e.getMessage(), containsString("Invalid value [0]. [num_allocations] must be a positive integer"));
        assertThat(e.getMessage(), containsString("Invalid value [-1]. [num_threads] must be a positive integer"));
    }

    public void testUpdateNumAllocations() {
        var testInstance = createTestInstance();
        var expectedNumAllocations = testInstance.getNumAllocations() != null ? testInstance.getNumAllocations() + 1 : 1;
        var updatedInstance = testInstance.updateServiceSettings(
            Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, expectedNumAllocations)
        );

        assertThat("update should create a new instance", updatedInstance, not(equalTo(testInstance)));
        assertThat(updatedInstance.getNumAllocations(), equalTo(expectedNumAllocations));
        assertThat(updatedInstance.getAdaptiveAllocationsSettings(), nullValue());
        assertThat(updatedInstance.getNumThreads(), equalTo(testInstance.getNumThreads()));
        assertThat(updatedInstance.getDeploymentId(), equalTo(testInstance.getDeploymentId()));
        assertThat(updatedInstance.modelId(), equalTo(testInstance.modelId()));

    }

    public void testUpdateAdaptiveAllocations() throws IOException {
        var testInstance = createTestInstance();
        var expectedAdaptiveAllocations = adaptiveAllocationSettings(testInstance.getAdaptiveAllocationsSettings());
        var updatedInstance = testInstance.updateServiceSettings(
            Map.of(ElasticsearchInternalServiceSettings.ADAPTIVE_ALLOCATIONS, toMap(expectedAdaptiveAllocations))
        );

        assertThat("update should create a new instance", updatedInstance, not(equalTo(testInstance)));
        assertThat(updatedInstance.getNumAllocations(), nullValue());
        assertThat(updatedInstance.getAdaptiveAllocationsSettings(), equalTo(expectedAdaptiveAllocations));
        assertThat(updatedInstance.getNumThreads(), equalTo(testInstance.getNumThreads()));
        assertThat(updatedInstance.getDeploymentId(), equalTo(testInstance.getDeploymentId()));
        assertThat(updatedInstance.modelId(), equalTo(testInstance.modelId()));
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
        assertThat(
            validationException.getMessage(),
            equalTo(
                "Validation Failed: 1: [service_settings] does not contain one of the required settings "
                    + "[num_allocations, adaptive_allocations];"
            )
        );
    }
}
