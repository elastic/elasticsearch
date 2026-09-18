/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SettingsScope;
import org.elasticsearch.xpack.inference.services.ocigenai.request.OciGenAiPrivateKeyParser;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalSecureString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredSecureString;
import static org.elasticsearch.xpack.inference.services.SettingsScope.SECRET_SETTINGS;
import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;

/**
 * The OCI API signing key used to authenticate requests to OCI Generative AI: the tenancy and user OCIDs, the fingerprint of the
 * public key uploaded to the user and the PEM encoded RSA private key. Together they form the OCI request signature
 * {@code keyId} ({@code <tenancy>/<user>/<fingerprint>}).
 *
 * @see <a href="https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm">Required keys and OCIDs</a>
 */
public class OciGenAiSecretSettings implements SecretSettings {

    public static final String NAME = "oci_genai_secret_settings";

    public static final String TENANCY_ID = "tenancy_id";
    public static final String USER_ID = "user_id";
    public static final String FINGERPRINT = "fingerprint";
    public static final String PRIVATE_KEY = "private_key";

    private final SecureString tenancyId;
    private final SecureString userId;
    private final SecureString fingerprint;
    private final SecureString privateKey;

    public static OciGenAiSecretSettings fromMap(@Nullable Map<String, Object> map, ConfigurationParseContext context) {
        if (map == null) {
            return null;
        }

        var validationException = new ValidationException();
        var scope = context == ConfigurationParseContext.REQUEST ? SERVICE_SETTINGS : SECRET_SETTINGS;

        var tenancyId = extractRequiredSecureString(map, TENANCY_ID, scope, validationException);
        var userId = extractRequiredSecureString(map, USER_ID, scope, validationException);
        var fingerprint = extractRequiredSecureString(map, FINGERPRINT, scope, validationException);
        var privateKey = extractRequiredSecureString(map, PRIVATE_KEY, scope, validationException);

        if (privateKey != null && context == ConfigurationParseContext.REQUEST) {
            validatePrivateKey(privateKey, scope, validationException);
        }

        validationException.throwIfValidationErrorsExist();

        return new OciGenAiSecretSettings(tenancyId, userId, fingerprint, privateKey);
    }

    private static void validatePrivateKey(SecureString privateKey, SettingsScope scope, ValidationException validationException) {
        try {
            OciGenAiPrivateKeyParser.parse(privateKey.toString());
        } catch (GeneralSecurityException e) {
            validationException.addValidationError(Strings.format("[%s] Invalid value for [%s]. %s", scope, PRIVATE_KEY, e.getMessage()));
        }
    }

    public OciGenAiSecretSettings(SecureString tenancyId, SecureString userId, SecureString fingerprint, SecureString privateKey) {
        this.tenancyId = Objects.requireNonNull(tenancyId);
        this.userId = Objects.requireNonNull(userId);
        this.fingerprint = Objects.requireNonNull(fingerprint);
        this.privateKey = Objects.requireNonNull(privateKey);
    }

    public OciGenAiSecretSettings(StreamInput in) throws IOException {
        this(in.readSecureString(), in.readSecureString(), in.readSecureString(), in.readSecureString());
    }

    public SecureString tenancyId() {
        return tenancyId;
    }

    public SecureString userId() {
        return userId;
    }

    public SecureString fingerprint() {
        return fingerprint;
    }

    public SecureString privateKey() {
        return privateKey;
    }

    /**
     * @return the OCI request signature key id: {@code <tenancy ocid>/<user ocid>/<fingerprint>}
     */
    public String keyId() {
        return tenancyId + "/" + userId + "/" + fingerprint;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TENANCY_ID, tenancyId.toString());
        builder.field(USER_ID, userId.toString());
        builder.field(FINGERPRINT, fingerprint.toString());
        builder.field(PRIVATE_KEY, privateKey.toString());
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return OciGenAiUtils.ML_INFERENCE_OCI_GENAI_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeSecureString(tenancyId);
        out.writeSecureString(userId);
        out.writeSecureString(fingerprint);
        out.writeSecureString(privateKey);
    }

    @Override
    public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
        var validationException = new ValidationException();
        var newTenancyId = extractOptionalSecureString(newSecrets, TENANCY_ID, SERVICE_SETTINGS, validationException);
        var newUserId = extractOptionalSecureString(newSecrets, USER_ID, SERVICE_SETTINGS, validationException);
        var newFingerprint = extractOptionalSecureString(newSecrets, FINGERPRINT, SERVICE_SETTINGS, validationException);
        var newPrivateKey = extractOptionalSecureString(newSecrets, PRIVATE_KEY, SERVICE_SETTINGS, validationException);
        if (newPrivateKey != null) {
            validatePrivateKey(newPrivateKey, SERVICE_SETTINGS, validationException);
        }
        validationException.throwIfValidationErrorsExist();

        var updated = new OciGenAiSecretSettings(
            Objects.requireNonNullElse(newTenancyId, tenancyId),
            Objects.requireNonNullElse(newUserId, userId),
            Objects.requireNonNullElse(newFingerprint, fingerprint),
            Objects.requireNonNullElse(newPrivateKey, privateKey)
        );
        return updated.equals(this) ? this : updated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OciGenAiSecretSettings that = (OciGenAiSecretSettings) o;
        return Objects.equals(tenancyId, that.tenancyId)
            && Objects.equals(userId, that.userId)
            && Objects.equals(fingerprint, that.fingerprint)
            && Objects.equals(privateKey, that.privateKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenancyId, userId, fingerprint, privateKey);
    }

    public static class Configuration {
        public static Map<String, SettingsConfiguration> get() {
            return CONFIGURATION.getOrCompute();
        }

        private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(
            TaskType.TEXT_EMBEDDING,
            TaskType.COMPLETION,
            TaskType.CHAT_COMPLETION,
            TaskType.RERANK
        );

        private static final LazyInitializable<Map<String, SettingsConfiguration>, RuntimeException> CONFIGURATION =
            new LazyInitializable<>(() -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();
                configurationMap.put(
                    TENANCY_ID,
                    secretConfiguration("The OCID of the OCI tenancy that owns the API signing key.", "Tenancy OCID")
                );
                configurationMap.put(USER_ID, secretConfiguration("The OCID of the OCI user the API signing key belongs to.", "User OCID"));
                configurationMap.put(
                    FINGERPRINT,
                    secretConfiguration("The fingerprint of the API signing key's public key, as shown in the OCI Console.", "Fingerprint")
                );
                configurationMap.put(
                    PRIVATE_KEY,
                    secretConfiguration(
                        "The PEM encoded RSA private key of the API signing key (PKCS#8 or PKCS#1, not passphrase protected).",
                        "Private Key"
                    )
                );
                return Collections.unmodifiableMap(configurationMap);
            });

        private static SettingsConfiguration secretConfiguration(String description, String label) {
            return new SettingsConfiguration.Builder(SUPPORTED_TASK_TYPES).setDescription(description)
                .setLabel(label)
                .setRequired(true)
                .setSensitive(true)
                .setUpdatable(true)
                .setType(SettingsConfigurationFieldType.STRING)
                .build();
        }
    }
}
