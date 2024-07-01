/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockSecretSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockServiceSettings;

import java.util.Objects;

public abstract class AmazonBedrockBaseClient extends AbstractRefCounted implements AmazonBedrockClient {
    protected final Integer modelKeysAndRegionHashcode;

    protected AmazonBedrockBaseClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        this.modelKeysAndRegionHashcode = getModelKeysAndRegionHashcode(model, timeout);
    }

    public static Integer getModelKeysAndRegionHashcode(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
        var serviceSettings = (AmazonBedrockServiceSettings) model.getServiceSettings();
        return Objects.hash(secretSettings.accessKey, secretSettings.secretKey, serviceSettings.region(), timeout);
    }
}
