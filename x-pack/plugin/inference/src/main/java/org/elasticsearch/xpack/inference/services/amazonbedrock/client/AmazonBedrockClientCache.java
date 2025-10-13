/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.io.Closeable;
import java.io.IOException;

public interface AmazonBedrockClientCache extends Closeable {
    AmazonBedrockBaseClient getOrCreateClient(AmazonBedrockModel model, @Nullable TimeValue timeout) throws IOException;
}
