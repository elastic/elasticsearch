/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.request;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GoogleAiStudioRequestTests extends ESTestCase {

    public void testDecorateWithApiKeyParameter() throws URISyntaxException {
        var uriString = "https://localhost:3000";
        var secureApiKey = new SecureString("api_key".toCharArray());
        var httpPost = new HttpPost(uriString);
        var secretSettings = new DefaultSecretSettings(secureApiKey);

        GoogleAiStudioRequest.decorateWithApiKeyParameter(httpPost, secretSettings);

        assertThat(httpPost.getURI(), is(new URI(Strings.format("%s?key=%s", uriString, secureApiKey))));
    }

    public void testDecorateWithApiKeyParameter_ThrowsValidationException_WhenAnyExceptionIsThrown() {
        var errorMessage = "something went wrong";
        var cause = new RuntimeException(errorMessage);
        var httpPost = mock(HttpPost.class);
        when(httpPost.getURI()).thenThrow(cause);

        ValidationException validationException = expectThrows(
            ValidationException.class,
            () -> GoogleAiStudioRequest.decorateWithApiKeyParameter(
                httpPost,
                new DefaultSecretSettings(new SecureString("abc".toCharArray()))
            )
        );
        assertThat(validationException.getCause(), is(cause));
        assertThat(validationException.getMessage(), containsString(errorMessage));
    }

}
