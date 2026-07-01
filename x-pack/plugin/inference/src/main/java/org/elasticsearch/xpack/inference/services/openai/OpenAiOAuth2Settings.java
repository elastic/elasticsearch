/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.oauth2.BaseOAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings.OAUTH2_SETTINGS_NOT_CONFIGURED_ERROR;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.TOKEN_URL;

/**
 * OpenAI-specific OAuth2 service settings: the standard client_credentials fields
 * (client_id, scopes) plus a customer-controlled token endpoint URL ({@code token_url}).
 *
 * <p>All three fields must be supplied together or omitted entirely. Mirrors the
 * shape of {@code AzureOpenAiOAuth2Settings} but with {@code token_url} replacing
 * Azure's {@code tenant_id}.
 */
public class OpenAiOAuth2Settings extends BaseOAuth2Settings {

    public static final TransportVersion OPENAI_OAUTH2_SETTINGS = TransportVersion.fromName("openai_oauth2_settings");

    public static final Set<String> REQUIRED_FIELDS = Sets.addToCopy(OAuth2Settings.REQUIRED_FIELDS, TOKEN_URL);
    public static final String REQUIRED_FIELDS_DESCRIPTION = requiredFieldsDescription(REQUIRED_FIELDS);

    private static final String SERVICE_DESCRIPTION = "OpenAI";

    private final URI tokenUrl;

    public static OpenAiOAuth2Settings fromMap(Map<String, Object> map, ValidationException validationException) {
        var oauth2ServiceSettings = OAuth2Settings.fromMap(map, validationException);
        var tokenUrl = extractOptionalUri(map, TOKEN_URL, validationException);

        var hasAllFields = validateFields(oauth2ServiceSettings, tokenUrl, TOKEN_URL, SERVICE_DESCRIPTION, validationException);

        if (hasAllFields) {
            return new OpenAiOAuth2Settings(oauth2ServiceSettings.result(), tokenUrl);
        }
        return null;
    }

    public OpenAiOAuth2Settings(String clientId, List<String> scopes, URI tokenUrl) {
        this(new OAuth2Settings(clientId, scopes), tokenUrl);
    }

    public OpenAiOAuth2Settings(OAuth2Settings oAuth2Settings, URI tokenUrl) {
        super(oAuth2Settings);
        this.tokenUrl = Objects.requireNonNull(tokenUrl);
    }

    public OpenAiOAuth2Settings(StreamInput in) throws IOException {
        super(new OAuth2Settings(in));
        this.tokenUrl = URI.create(in.readString());
    }

    public URI tokenUrl() {
        return tokenUrl;
    }

    /**
     * Validates that the OAuth2 settings are attempting to be added. We only allow updating the OAuth2 settings if they were configured
     * when the endpoint was created.
     * @param currentSettings the current OAuth2 settings, can be null if the endpoint was not configured with OAuth2 settings
     * @param serviceSettingsMap the map of settings to update, used to check if there are any OAuth2 fields attempting to be added
     * @param validationException the validation exception to add any errors to if there are OAuth2 fields attempting to be added
     *                            when there were no OAuth2 settings previously configured
     * @return the updated OAuth2 settings if there are any updates, or the current settings if there are no updates.
     * Will return null if there were attempted updates to OAuth2 settings when there were no OAuth2 settings previously configured,
     * as that is not allowed.
     */
    public static OpenAiOAuth2Settings updateServiceSettingsIfPresent(
        @Nullable OpenAiOAuth2Settings currentSettings,
        Map<String, Object> serviceSettingsMap,
        ValidationException validationException
    ) {
        var updatedOAuth2Settings = currentSettings;
        if (currentSettings == null && OpenAiOAuth2Settings.hasAnyOAuth2Fields(serviceSettingsMap)) {
            validationException.addValidationError(OAUTH2_SETTINGS_NOT_CONFIGURED_ERROR);
        } else if (currentSettings != null) {
            updatedOAuth2Settings = currentSettings.updateServiceSettings(serviceSettingsMap, validationException);
        }

        return updatedOAuth2Settings;
    }

    private static boolean hasAnyOAuth2Fields(Map<String, Object> map) {
        return OAuth2Settings.hasAnyOAuth2Fields(map) || map.containsKey(TOKEN_URL);
    }

    public OpenAiOAuth2Settings updateServiceSettings(Map<String, Object> serviceSettingsMap, ValidationException validationException) {
        var updatedOauth2 = oAuth2Settings.updateServiceSettings(serviceSettingsMap, validationException);
        var newTokenUrlString = extractOptionalString(
            serviceSettingsMap,
            TOKEN_URL,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var newTokenUrl = newTokenUrlString == null
            ? this.tokenUrl
            : convertToUri(newTokenUrlString, TOKEN_URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        return new OpenAiOAuth2Settings(updatedOauth2, newTokenUrl);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        oAuth2Settings.writeTo(out);
        out.writeString(tokenUrl.toString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        oAuth2Settings.toXContent(builder, params);
        builder.field(TOKEN_URL, tokenUrl.toString());
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (OpenAiOAuth2Settings) o;
        return Objects.equals(oAuth2Settings, that.oAuth2Settings) && Objects.equals(tokenUrl, that.tokenUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oAuth2Settings, tokenUrl);
    }
}
