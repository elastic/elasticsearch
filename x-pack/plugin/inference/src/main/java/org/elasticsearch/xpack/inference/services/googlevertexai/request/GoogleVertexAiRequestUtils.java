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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public final class GoogleVertexAiRequestUtils {
    private static final List<String> AUTH_SCOPE = Collections.singletonList("https://www.googleapis.com/auth/cloud-platform");

    /**
     * Attaches a bearer token to the given request, authenticating either with an explicit
     * service account key or, when {@code secretSettings} is {@code null} (the key was omitted
     * at endpoint creation), with Application Default Credentials. ADC resolves credentials
     * ambiently from the environment, for example the GCE/GKE metadata server when Workload
     * Identity is configured. Note that an omitted key is represented as {@code null}, whereas
     * an empty key is rejected during settings validation and never reaches this method.
     */
    public static void decorateWithBearerToken(HttpPost httpPost, @Nullable GoogleVertexAiSecretSettings secretSettings) {
        try {
            GoogleCredentials credentials = resolveCredentials(secretSettings).createScoped(AUTH_SCOPE);
            credentials.refreshIfExpired();
            httpPost.setHeader(createAuthBearerHeader(new SecureString(credentials.getAccessToken().getTokenValue().toCharArray())));
        } catch (Exception e) {
            throw new ElasticsearchStatusException(e.getMessage(), RestStatus.FORBIDDEN, e);
        }
    }

    private static GoogleCredentials resolveCredentials(@Nullable GoogleVertexAiSecretSettings secretSettings) throws IOException {
        if (secretSettings == null) {
            return GoogleCredentials.getApplicationDefault();
        }
        return ServiceAccountCredentials.fromStream(
            new ByteArrayInputStream(secretSettings.serviceAccountJson().toString().getBytes(StandardCharsets.UTF_8))
        );
    }

    private GoogleVertexAiRequestUtils() {}
}
