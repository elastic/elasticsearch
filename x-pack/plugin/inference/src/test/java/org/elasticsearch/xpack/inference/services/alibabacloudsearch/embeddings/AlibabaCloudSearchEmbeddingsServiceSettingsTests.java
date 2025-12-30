/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<
    AlibabaCloudSearchEmbeddingsServiceSettings> {

    public static final SimilarityMeasure SIMILARITY_MEASURE_VALUE = SimilarityMeasure.DOT_PRODUCT;
    public static final int DIMS_VALUE = 1536;
    public static final int MAX_INPUT_TOKENS_VALUE = 512;
    public static final String SERVICE_ID_VALUE = "model";
    public static final String HOST_VALUE = "host";
    public static final String WORKSPACE_NAME_VALUE = "default";
    public static final String HTTP_SCHEMA_VALUE = "https";
    private static final long RATE_LIMIT_VALUE = 1L;

    public static AlibabaCloudSearchEmbeddingsServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchEmbeddingsServiceSettings(
            commonSettings,
            SIMILARITY_MEASURE_VALUE,
            DIMS_VALUE,
            MAX_INPUT_TOKENS_VALUE
        );
    }

    public void testFromMap() {
        var serviceSettings = AlibabaCloudSearchEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    ServiceFields.DIMENSIONS,
                    DIMS_VALUE,
                    ServiceFields.MAX_INPUT_TOKENS,
                    MAX_INPUT_TOKENS_VALUE,
                    AlibabaCloudSearchServiceSettings.HOST,
                    HOST_VALUE,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    SERVICE_ID_VALUE,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    WORKSPACE_NAME_VALUE,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    HTTP_SCHEMA_VALUE
                )
            ),
            null
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchEmbeddingsServiceSettings(
                    new AlibabaCloudSearchServiceSettings(SERVICE_ID_VALUE, HOST_VALUE, WORKSPACE_NAME_VALUE, HTTP_SCHEMA_VALUE, null),
                    SIMILARITY_MEASURE_VALUE,
                    DIMS_VALUE,
                    MAX_INPUT_TOKENS_VALUE
                )
            )
        );
    }

    public void testUpdateServiceSettings_AllFields_Success() {
        var serviceSettings = new AlibabaCloudSearchEmbeddingsServiceSettings(
            AlibabaCloudSearchServiceSettingsTests.createRandom(),
            SimilarityMeasure.COSINE,
            1,
            1
        ).updateServiceSettings(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    ServiceFields.DIMENSIONS,
                    DIMS_VALUE,
                    ServiceFields.MAX_INPUT_TOKENS,
                    MAX_INPUT_TOKENS_VALUE,
                    AlibabaCloudSearchServiceSettings.HOST,
                    HOST_VALUE,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    SERVICE_ID_VALUE,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    WORKSPACE_NAME_VALUE,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    HTTP_SCHEMA_VALUE,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                )
            ),
            TaskType.TEXT_EMBEDDING
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchEmbeddingsServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        SERVICE_ID_VALUE,
                        HOST_VALUE,
                        WORKSPACE_NAME_VALUE,
                        HTTP_SCHEMA_VALUE,
                        new RateLimitSettings(RATE_LIMIT_VALUE)
                    ),
                    SIMILARITY_MEASURE_VALUE,
                    DIMS_VALUE,
                    MAX_INPUT_TOKENS_VALUE
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<AlibabaCloudSearchEmbeddingsServiceSettings> instanceReader() {
        return AlibabaCloudSearchEmbeddingsServiceSettings::new;
    }

    @Override
    protected AlibabaCloudSearchEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchEmbeddingsServiceSettings mutateInstance(AlibabaCloudSearchEmbeddingsServiceSettings instance)
        throws IOException {
        return null;
    }

    public static Map<String, Object> getServiceSettingsMap(String serviceId, String host, String workspaceName) {
        return AlibabaCloudSearchServiceSettingsTests.getServiceSettingsMap(serviceId, host, workspaceName);
    }
}
