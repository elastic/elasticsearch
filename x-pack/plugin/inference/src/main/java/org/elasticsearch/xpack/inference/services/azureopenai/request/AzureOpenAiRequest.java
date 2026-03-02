/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredentialBuilder;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings;

import java.time.Duration;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.API_KEY;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.ENTRA_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;

public interface AzureOpenAiRequest extends Request {

    String MISSING_AUTHENTICATION_ERROR_MESSAGE =
        "The request does not have any authentication methods set. One of [%s] or [%s] is required.";

    static void decorateWithAuthHeader(HttpPost httpPost, AzureOpenAiSecretSettings secretSettings) {
        TokenCredential credential = new ClientSecretCredentialBuilder().tenantId("")
            .clientId("")
            .clientSecret("")
            .build();

        // 2. Define the Token Request Context
        // This tells the credential what resource/scope you are requesting a token for.
        TokenRequestContext context = new TokenRequestContext().setScopes(List.of(""));

        // 3. Retrieve the Access Token
        // We use .block() for a simple synchronous call. In a production environment
        // where performance is critical, you should handle the Mono asynchronously.
        // AccessToken accessToken = credential.getToken(context).block(Duration.ofSeconds(10));
        var token = credential.getToken(context).block(Duration.ofSeconds(10));
        httpPost.setHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType()));

        var entraId = secretSettings.entraId();
        var apiKey = secretSettings.apiKey();

        if (entraId != null && entraId.isEmpty() == false) {
            httpPost.setHeader(createAuthBearerHeader(entraId));
        } else if (apiKey != null && apiKey.isEmpty() == false) {
            httpPost.setHeader(new BasicHeader(API_KEY_HEADER, apiKey.toString()));
        } else {
            // should never happen due to the checks on the secret settings, but just in case
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(Strings.format(MISSING_AUTHENTICATION_ERROR_MESSAGE, API_KEY, ENTRA_ID));
            throw validationException;
        }
    }
}
