/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<
    AlibabaCloudSearchEmbeddingsServiceSettings> {
    public static AlibabaCloudSearchEmbeddingsServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        var similarity = SimilarityMeasure.DOT_PRODUCT;
        var dims = 1536;
        var maxInputTokens = 512;
        return new AlibabaCloudSearchEmbeddingsServiceSettings(commonSettings, similarity, dims, maxInputTokens);
    }

    public void testFromMap() {
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var host = "host";
        var workspaceName = "default";
        var httpSchema = "https";
        var serviceSettings = AlibabaCloudSearchEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    AlibabaCloudSearchServiceSettings.HOST,
                    host,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    model,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    workspaceName,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    httpSchema
                )
            ),
            null
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchEmbeddingsServiceSettings(
                    new AlibabaCloudSearchServiceSettings(model, host, workspaceName, httpSchema, null),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens
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
