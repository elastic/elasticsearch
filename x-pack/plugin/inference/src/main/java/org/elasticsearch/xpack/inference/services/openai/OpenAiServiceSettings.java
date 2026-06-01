/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for OpenAI service settings, containing fields common to all OpenAI service setting types
 * (e.g. completion, embeddings).
 */
public abstract class OpenAiServiceSettings extends FilteredXContentObject implements ServiceSettings {

    /**
     * Settings common to all OpenAI service setting types. Only OAuth2 for now, but structured so
     * other shared fields (model id, url, rate limit, ...) can move here later without reworking
     * the subclasses.
     */
    protected record CommonSettings(@Nullable OpenAiOAuth2Settings oAuth2Settings) {}

    protected final OpenAiOAuth2Settings oAuth2Settings;

    protected OpenAiServiceSettings(@Nullable OpenAiOAuth2Settings oAuth2Settings) {
        this.oAuth2Settings = oAuth2Settings;
    }

    public OpenAiOAuth2Settings oAuth2Settings() {
        return oAuth2Settings;
    }

    /**
     * Parses the common OpenAI service settings from a map. Subclasses may use this when implementing
     * their own {@code fromMap} and then parse additional fields.
     */
    protected static CommonSettings parseCommonSettings(Map<String, Object> map, ValidationException validationException) {
        return new CommonSettings(OpenAiOAuth2Settings.fromMap(map, validationException));
    }

    /**
     * Applies an update map to the current common settings. Rejects adding OAuth2 fields when the service
     * was not originally configured with OAuth2 settings.
     */
    protected CommonSettings updateCommonSettings(Map<String, Object> serviceSettings, ValidationException validationException) {
        return new CommonSettings(
            OpenAiOAuth2Settings.updateServiceSettingsIfPresent(oAuth2Settings, serviceSettings, validationException)
        );
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (oAuth2Settings != null) {
            oAuth2Settings.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        OpenAiServiceSettings that = (OpenAiServiceSettings) o;
        return Objects.equals(oAuth2Settings, that.oAuth2Settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oAuth2Settings);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
