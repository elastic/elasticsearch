/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.voyageai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public record VoyageAIAccount(URI uri, SecureString apiKey) {

    public static VoyageAIAccount of(VoyageAIModel model) {
        try {
            var uri = model.buildUri();
            return new VoyageAIAccount(uri, model.apiKey());
        } catch (URISyntaxException e) {
            // using bad request here so that potentially sensitive URL information does not get logged
            throw new ElasticsearchStatusException("Failed to construct VoyageAI URL", RestStatus.BAD_REQUEST, e);
        }
    }

    public VoyageAIAccount {
        Objects.requireNonNull(uri);
        Objects.requireNonNull(apiKey);
    }
}
