/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public interface GoogleVertexAiRequest extends Request {
    List<String> AUTH_SCOPE = Collections.singletonList("https://www.googleapis.com/auth/cloud-platform");

    static void decorateWithBearerToken(HttpPost httpPost, GoogleVertexAiSecretSettings secretSettings) {
        SpecialPermission.check();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                GoogleCredentials credentials = ServiceAccountCredentials.fromStream(
                    new ByteArrayInputStream(secretSettings.serviceAccountJson().toString().getBytes(StandardCharsets.UTF_8))
                ).createScoped(AUTH_SCOPE);
                credentials.refreshIfExpired();
                httpPost.setHeader(createAuthBearerHeader(new SecureString(credentials.getAccessToken().getTokenValue().toCharArray())));

                return null;
            });
        } catch (Exception e) {
            throw new ElasticsearchStatusException(e.getMessage(), RestStatus.FORBIDDEN, e);
        }
    }
}
