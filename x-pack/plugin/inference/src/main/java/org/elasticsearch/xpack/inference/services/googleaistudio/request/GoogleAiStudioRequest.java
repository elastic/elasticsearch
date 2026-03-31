/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.request;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;

public interface GoogleAiStudioRequest extends Request {

    String API_KEY_PARAMETER = "key";

    static void decorateWithApiKeyParameter(HttpPost httpPost, DefaultSecretSettings secretSettings) {
        try {
            var uriWithApiKey = builderWithApiKeyParameter(httpPost.getURI(), secretSettings).build();
            httpPost.setURI(uriWithApiKey);
        } catch (Exception e) {
            ValidationException validationException = new ValidationException(e);
            validationException.addValidationError(e.getMessage());
            throw validationException;
        }
    }

    static URIBuilder builderWithApiKeyParameter(URI uri, DefaultSecretSettings secretSettings) {
        return new URIBuilder().setScheme(uri.getScheme())
            .setHost(uri.getHost())
            .setPort(uri.getPort())
            .setPath(uri.getPath())
            .addParameter(API_KEY_PARAMETER, secretSettings.apiKey().toString());
    }

}
