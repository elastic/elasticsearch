/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.cluster.metadata;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.cluster.crypto.CryptoSettings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Metadata about encryption and decryption
 *
 * @opensearch.experimental
 */
public class CryptoMetadata implements Writeable {
    @SuppressWarnings("checkstyle:ModifierOrder")
    static final public String CRYPTO_METADATA_KEY = "crypto_metadata";
    @SuppressWarnings("checkstyle:ModifierOrder")
    static final public String KEY_PROVIDER_NAME_KEY = "key_provider_name";
    @SuppressWarnings("checkstyle:ModifierOrder")
    static final public String KEY_PROVIDER_TYPE_KEY = "key_provider_type";
    @SuppressWarnings("checkstyle:ModifierOrder")
    static final public String SETTINGS_KEY = "settings";
    private final String keyProviderName;
    private final String keyProviderType;
    private final Settings settings;

    /**
     * Constructs new crypto metadata
     *
     * @param keyProviderName     key provider name
     * @param keyProviderType     key provider type
     * @param settings crypto settings
     */
    public CryptoMetadata(String keyProviderName, String keyProviderType, Settings settings) {
        this.keyProviderName = keyProviderName;
        this.keyProviderType = keyProviderType;
        this.settings = settings;
    }

    /**
     * Returns key provider name
     *
     * @return Key provider name
     */
    public String keyProviderName() {
        return this.keyProviderName;
    }

    /**
     * Returns key provider type
     *
     * @return key provider type
     */
    public String keyProviderType() {
        return this.keyProviderType;
    }

    /**
     * Returns crypto settings
     *
     * @return crypto settings
     */
    public Settings settings() {
        return this.settings;
    }

    public CryptoMetadata(StreamInput in) throws IOException {
        keyProviderName = in.readString();
        keyProviderType = in.readString();
        settings = Settings.readSettingsFromStream(in);
    }

    public static CryptoMetadata fromRequest(CryptoSettings cryptoSettings) {
        if (cryptoSettings == null) {
            return null;
        }
        return new CryptoMetadata(cryptoSettings.getKeyProviderName(), cryptoSettings.getKeyProviderType(), cryptoSettings.getSettings());
    }

    /**
     * Writes crypto metadata to stream output
     *
     * @param out stream output
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(keyProviderName);
        out.writeString(keyProviderType);
        Settings.writeSettingsToStream(settings, out);
    }

    public static CryptoMetadata fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String keyProviderType = null;
        Settings settings = null;
        String keyProviderName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if (KEY_PROVIDER_NAME_KEY.equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                        throw new ElasticsearchParseException("failed to parse crypto metadata [{}], unknown type");
                    }
                    keyProviderName = parser.text();
                } else if (KEY_PROVIDER_TYPE_KEY.equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                        throw new ElasticsearchParseException("failed to parse crypto metadata [{}], unknown type");
                    }
                    keyProviderType = parser.text();
                } else if (SETTINGS_KEY.equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new ElasticsearchParseException("failed to parse crypto metadata [{}], unknown type");
                    }
                    settings = Settings.fromXContent(parser);
                } else {
                    throw new ElasticsearchParseException("failed to parse crypto metadata, unknown field [{}]", currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("failed to parse repositories");
            }
        }
        return new CryptoMetadata(keyProviderName, keyProviderType, settings);
    }

    public void toXContent(CryptoMetadata cryptoMetadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(CRYPTO_METADATA_KEY);
        builder.field(KEY_PROVIDER_NAME_KEY, cryptoMetadata.keyProviderName());
        builder.field(KEY_PROVIDER_TYPE_KEY, cryptoMetadata.keyProviderType());
        builder.startObject(SETTINGS_KEY);
        cryptoMetadata.settings().toXContent(builder, params);
        builder.endObject();
        builder.endObject();
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CryptoMetadata that = (CryptoMetadata) o;

        if (!keyProviderName.equals(that.keyProviderName)) return false;
        if (!keyProviderType.equals(that.keyProviderType)) return false;
        return settings.equals(that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyProviderName, keyProviderType, settings);
    }

    @Override
    public String toString() {
        return "CryptoMetadata{" + keyProviderName + "}{" + keyProviderType + "}{" + settings + "}";
    }
}
