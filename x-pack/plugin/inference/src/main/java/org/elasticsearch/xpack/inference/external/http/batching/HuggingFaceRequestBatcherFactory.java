/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.apache.http.client.protocol.HttpClientContext;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;

public record HuggingFaceRequestBatcherFactory(BatchingComponents components) implements RequestBatcherFactory<HuggingFaceAccount> {

    @Override
    public RequestBatcher<HuggingFaceAccount> create(HttpClientContext context) {
        return new BaseRequestBatcher<>(components, context);
    }
}
