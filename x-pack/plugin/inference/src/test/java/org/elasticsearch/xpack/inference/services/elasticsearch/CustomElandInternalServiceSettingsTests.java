/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public class CustomElandInternalServiceSettingsTests extends AbstractElasticsearchInternalServiceSettingsTests<
    CustomElandInternalServiceSettings> {

    public static CustomElandInternalServiceSettings createRandom() {
        return new CustomElandInternalServiceSettings(ElasticsearchInternalServiceSettingsTests.validInstance(randomAlphaOfLength(10)));
    }

    @Override
    protected Writeable.Reader<CustomElandInternalServiceSettings> instanceReader() {
        return CustomElandInternalServiceSettings::new;
    }

    @Override
    protected CustomElandInternalServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CustomElandInternalServiceSettings mutateInstance(CustomElandInternalServiceSettings instance) throws IOException {
        return new CustomElandInternalServiceSettings(ElasticsearchInternalServiceSettingsTests.doMutateInstance(instance));
    }

    @Override
    protected void assertUpdated(CustomElandInternalServiceSettings original, CustomElandInternalServiceSettings updated) {
        // Nothing to do as there are no additional properties
    }
}
