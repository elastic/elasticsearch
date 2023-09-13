/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;

public class OpenAiEmbeddingsRequest {
    // TODO this should create the actual request to send via the http client
    // similar to IncidentEvent

    // TODO this should be given an openai account so it can access the api key

    private final OpenAiAccount account;

    public OpenAiEmbeddingsRequest(OpenAiAccount account) {
        this.account = account;
    }

}
