/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.oauth;

import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretsSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettings;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.azureopenai.oauth.AzureOpenAiOAuthSettings.AZURE_OPENAI_OAUTH_SETTINGS;
import static org.elasticsearch.xpack.inference.services.azureopenai.oauth.AzureOpenAiOAuthSettings.REQUIRED_FIELDS;
import static org.elasticsearch.xpack.inference.services.azureopenai.oauth.AzureOpenAiOAuthSettings.REQUIRED_FIELDS_DESCRIPTION;

public class AzureOpenAiOAuth2Secrets extends AzureOpenAiSecretsSettings {

    public static final String NAME = "azure_openai_oauth2_client_secret";

    public static final String CLIENT_SECRET_FIELD = "client_secret";

    public static final String USE_CLIENT_SECRET_ERROR = Strings.format(
        "To use OAuth2 the [%s] field must be set, " + "either remove fields [%s], or provide the [%s] field.",
        CLIENT_SECRET_FIELD,
        REQUIRED_FIELDS,
        CLIENT_SECRET_FIELD
    );

    private final SecureString clientSecret;
    private TokenRequestContext tokenRequestContext;
    private ClientSecretCredential credential;

    public AzureOpenAiOAuth2Secrets(String inferenceId, SecureString clientSecrets) {
        super(inferenceId);

        this.clientSecret = Objects.requireNonNull(clientSecrets);
    }

    public AzureOpenAiOAuth2Secrets(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalSecureString());
    }

    @Override
    public void init(AzureOpenAiServiceSettings serviceSettings) {
        if (serviceSettings.oAuth2Settings() == null) {
            throw new ValidationException().addValidationError(REQUIRED_FIELDS_DESCRIPTION);
        }

        credential = new ClientSecretCredentialBuilder().tenantId(serviceSettings.oAuth2Settings().getTenantId())
            .clientId(serviceSettings.oAuth2Settings().getClientId())
            .clientSecret(clientSecret.toString())
            .build();

        tokenRequestContext = new TokenRequestContext().setScopes(serviceSettings.oAuth2Settings().getScopes());
    }

    // TODO maybe make this default visibility only for testing
    public SecureString getClientSecret() {
        return clientSecret;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener) {
        assert credential != null && tokenRequestContext != null : "init() must be called before retrieving access token for OAuth2";

        try {
            credential.getToken(tokenRequestContext).subscribe(token -> {
                String authorizationHeader = "Bearer " + token.getToken();

                request.setHeader(HttpHeaders.AUTHORIZATION, authorizationHeader);
                listener.onResponse(request);
            },
                e -> listener.onFailure(
                    new ElasticsearchException(
                        Strings.format("Failed to retrieve access token for Azure OpenAI request for inference id: [%s]", inferenceId),
                        e
                    )
                )
            );
        } catch (Exception e) {
            listener.onFailure(
                new ElasticsearchException(
                    Strings.format(
                        "Failed attempting to retrieve access token for Azure OpenAI request for inference id: [%s]",
                        inferenceId
                    ),
                    e
                )
            );
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return AZURE_OPENAI_OAUTH_SETTINGS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(inferenceId);
        out.writeSecureString(clientSecret);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLIENT_SECRET_FIELD, clientSecret.toString());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiOAuth2Secrets that = (AzureOpenAiOAuth2Secrets) o;
        return Objects.equals(clientSecret, that.clientSecret) && Objects.equals(inferenceId, that.inferenceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientSecret, inferenceId);
    }
}
