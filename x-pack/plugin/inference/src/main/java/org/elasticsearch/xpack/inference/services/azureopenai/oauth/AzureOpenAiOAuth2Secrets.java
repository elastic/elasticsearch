/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.oauth;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.TransportVersion;
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

    public AzureOpenAiOAuth2Secrets(SecureString clientSecrets) {
        this.clientSecret = Objects.requireNonNull(clientSecrets);
    }

    public AzureOpenAiOAuth2Secrets(StreamInput in) throws IOException {
        this(in.readOptionalSecureString());
    }

    public SecureString getClientSecret() {
        return clientSecret;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void applyTo(HttpRequestBase request) {
        // TODO
    }

    @Override
    protected void validateServiceSettings(AzureOpenAiServiceSettings serviceSettings) {
        if (serviceSettings.oAuth2Settings() == null) {
            throw new ValidationException().addValidationError(REQUIRED_FIELDS_DESCRIPTION);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return AZURE_OPENAI_OAUTH_SETTINGS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeSecureString(clientSecret);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLIENT_SECRET_FIELD, clientSecret.toString());
        builder.endObject();
        return builder;
    }
}
