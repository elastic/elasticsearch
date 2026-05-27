/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;

public class CohereCommonServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CohereCommonServiceSettings> {

    public static CohereCommonServiceSettings createRandom() {
        return new CohereCommonServiceSettings(
            randomUriOrNull(),
            randomAlphaOfLengthOrNull(15),
            RateLimitSettingsTests.createRandom(),
            randomFrom(CohereCommonServiceSettings.CohereApiVersion.values())
        );
    }

    private static URI randomUriOrNull() {
        return randomBoolean() ? null : ServiceUtils.createUri(randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<CohereCommonServiceSettings> instanceReader() {
        return CohereCommonServiceSettings::new;
    }

    @Override
    protected CohereCommonServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereCommonServiceSettings mutateInstance(CohereCommonServiceSettings instance) throws IOException {
        var uri = instance.uri();
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        var apiVersion = instance.apiVersion();

        switch (randomInt(3)) {
            case 0 -> uri = randomValueOtherThan(uri, CohereCommonServiceSettingsTests::randomUriOrNull);
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(15));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 3 -> apiVersion = randomValueOtherThan(
                apiVersion,
                () -> randomFrom(CohereCommonServiceSettings.CohereApiVersion.values())
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CohereCommonServiceSettings(uri, modelId, rateLimitSettings, apiVersion);
    }

    @Override
    protected CohereCommonServiceSettings mutateInstanceForVersion(CohereCommonServiceSettings instance, TransportVersion version) {
        return instance;
    }
}
