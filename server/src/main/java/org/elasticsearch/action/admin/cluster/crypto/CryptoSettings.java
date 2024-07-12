/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.action.admin.cluster.crypto;


import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.annotation.PublicApi;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

/**
 * Crypto settings supplied during a put repository request
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CryptoSettings implements Writeable, ToXContentObject {
    private String keyProviderName;
    private String keyProviderType;
    private Settings settings = EMPTY_SETTINGS;

    public CryptoSettings(StreamInput in) throws IOException {
        keyProviderName = in.readString();
        keyProviderType = in.readString();
        settings = readSettingsFromStream(in);
    }

    public CryptoSettings(String keyProviderName) {
        this.keyProviderName = keyProviderName;
    }

    /**
     * Validate settings supplied in put repository request.
     * @return Exception in case validation fails.
     */
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (keyProviderName == null) {
            validationException = addValidationError("key_provider_name is missing", validationException);
        }
        if (keyProviderType == null) {
            validationException = addValidationError("key_provider_type is missing", validationException);
        }
        return validationException;
    }

    /**
     * Returns key provider name
     * @return keyProviderName
     */
    public String getKeyProviderName() {
        return keyProviderName;
    }

    /**
     * Returns key provider type
     * @return keyProviderType
     */
    public String getKeyProviderType() {
        return keyProviderType;
    }

    /**
     * Returns crypto settings
     * @return settings
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * Constructs a new crypto settings with provided key provider name.
     * @param keyProviderName Name of the key provider
     */
    public CryptoSettings keyProviderName(String keyProviderName) {
        this.keyProviderName = keyProviderName;
        return this;
    }

    /**
     * Constructs a new crypto settings with provided key provider type.
     * @param keyProviderType Type of key provider to be used in encryption.
     */
    public CryptoSettings keyProviderType(String keyProviderType) {
        this.keyProviderType = keyProviderType;
        return this;
    }

    /**
     * Sets the encryption settings
     *
     * @param settings for encryption
     * @return this request
     */
    public CryptoSettings settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets the encryption settings.
     *
     * @param source encryption settings in json or yaml format
     * @param xContentType the content type of the source
     * @return this request
     */
    public CryptoSettings settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets the encryption settings.
     *
     * @param source encryption settings
     * @return this request
     */
    public CryptoSettings settings(Map<String, Object> source) {
        this.settings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    /**
     * Parses crypto settings definition.
     *
     * @param cryptoDefinition crypto settings definition
     */
    @SuppressWarnings("checkstyle:DescendantToken")
    public CryptoSettings(Map<String, Object> cryptoDefinition) {
        for (Map.Entry<String, Object> entry : cryptoDefinition.entrySet()) {
            if (entry.getKey().equals("key_provider_name")) {
                keyProviderName(entry.getValue().toString());
            } else if (entry.getKey().equals("key_provider_type")) {
                keyProviderType(entry.getValue().toString());
            } else if (entry.getKey().equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("Malformed settings section in crypto settings, should include an inner object");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> sub = (Map<String, Object>) entry.getValue();
                settings(sub);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(keyProviderName);
        out.writeString(keyProviderType);
        writeSettingsToStream(settings, out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("key_provider_name", keyProviderName);
        builder.field("key_provider_type", keyProviderType);

        builder.startObject("settings");
        settings.toXContent(builder, params);
        builder.endObject();

        builder.endObject();
        return builder;
    }
}
