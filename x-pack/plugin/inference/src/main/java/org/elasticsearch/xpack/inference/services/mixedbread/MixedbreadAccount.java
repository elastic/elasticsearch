/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public record MixedbreadAccount(URI baseUri, SecureString apiKey) {

    public static MixedbreadAccount of(MixedbreadModel model) {
        try {
            var uri = model.baseUri() != null ? model.baseUri() : new URIBuilder().setScheme("https").setHost("api.mixedbread.com").build();
            return new MixedbreadAccount(uri, model.apiKey());
        } catch (URISyntaxException e) {
            // using bad request here so that potentially sensitive URL information does not get logged
            throw new ElasticsearchStatusException(
                Strings.format("Failed to construct %s URL", MixedbreadService.NAME),
                RestStatus.BAD_REQUEST,
                e
            );
        }
    }

    public MixedbreadAccount {
        Objects.requireNonNull(baseUri);
        Objects.requireNonNull(apiKey);
    }
}
