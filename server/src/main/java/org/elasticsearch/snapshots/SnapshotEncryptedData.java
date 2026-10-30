/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The {@code encrypted_data} block from a create-snapshot request. Carries a type discriminator
 * ({@value #TYPE_PASSWORD} or {@value #TYPE_SECURE_SETTING}) and type-specific fields.
 *
 * <p>Kept in {@link SnapshotsInProgressEncryptedData} so that any master can finalize the snapshot, so the password must never appear
 * in {@link #toString()} or any XContent output.
 */
public final class SnapshotEncryptedData implements Writeable {

    public static final TransportVersion TRANSPORT_VERSION = TransportVersion.fromName("snapshot_encrypted_data");
    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("snapshot_encrypted_data");

    public static final String TYPE_PASSWORD = "password";
    public static final String TYPE_SECURE_SETTING = "secure_setting";
    /** Recorded in {@link SnapshotInfo} for a snapshot that excluded the cluster's encrypted data because no password was supplied. */
    public static final String TYPE_NONE = "none";

    /** Sentinel value rejected by {@link #validatePassword}. */
    static final String REDACTED_SENTINEL = "::es_redacted::";

    private static final int MIN_PASSWORD_LENGTH = 15;
    // bounds the size of the in-progress snapshot custom that carries the password in cluster state
    static final int MAX_PASSWORD_LENGTH = 1024;

    // a secure_setting password_id becomes part of a setting name
    private static final Pattern PASSWORD_ID_PATTERN = Pattern.compile("[A-Za-z0-9_-]{1,128}");

    // Reject passwords that are *entirely* a template fragment via Matcher#matches
    private static final Map<Pattern, String> TEMPLATE_PATTERNS = Map.of(
        Pattern.compile("\\s*\\$\\{[^}]+\\}\\s*"),
        "${VAR}",
        Pattern.compile("\\s*\\$[A-Za-z_][A-Za-z0-9_]*\\s*"),
        "$VAR",
        Pattern.compile("\\s*\\{\\{[^}]+\\}\\}\\s*"),
        "{{ .var }}",
        Pattern.compile("\\s*%[A-Za-z_][A-Za-z0-9_]*%\\s*"),
        "%VAR%"
    );
    private static final String TEMPLATE_EXAMPLES = TEMPLATE_PATTERNS.values().stream().sorted().collect(Collectors.joining(", "));

    private final String type;

    @Nullable
    private final SecureString password;

    @Nullable
    private final String passwordId;

    public SnapshotEncryptedData(String type, @Nullable SecureString password, @Nullable String passwordId) {
        this.type = Objects.requireNonNull(type);
        this.password = password;
        this.passwordId = passwordId;
    }

    public SnapshotEncryptedData(StreamInput in) throws IOException {
        this.type = in.readString();
        this.password = in.readOptionalSecureString();
        this.passwordId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeOptionalSecureString(password);
        out.writeOptionalString(passwordId);
    }

    public String type() {
        return type;
    }

    @Nullable
    public SecureString password() {
        return password;
    }

    @Nullable
    public String passwordId() {
        return passwordId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotEncryptedData that = (SnapshotEncryptedData) o;
        return type.equals(that.type) && Objects.equals(password, that.password) && Objects.equals(passwordId, that.passwordId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, password, passwordId);
    }

    @Override
    public String toString() {
        return "SnapshotEncryptedData{type=" + type + ", passwordId=" + passwordId + "}";
    }

    /**
     * Parses the value of the {@code encrypted_data} key in the request body. Throws {@link IllegalArgumentException} for malformed
     * input, missing required fields, or unsupported types.
     */
    @SuppressWarnings("unchecked")
    public static SnapshotEncryptedData fromMap(Object value) {
        if (value instanceof Map == false) {
            throw new IllegalArgumentException("malformed encrypted_data, should be an object");
        }
        Map<String, Object> map = (Map<String, Object>) value;
        String type = null;
        String password = null;
        String passwordId = null;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            switch (entry.getKey()) {
                case "type" -> type = stringValue(entry);
                case "password" -> password = stringValue(entry);
                case "password_id" -> passwordId = entry.getValue() != null ? stringValue(entry) : null;
                default -> throw new IllegalArgumentException("unknown field [encrypted_data." + entry.getKey() + "]");
            }
        }
        if (type == null) {
            throw new IllegalArgumentException("encrypted_data.type is required");
        }
        return switch (type) {
            case TYPE_SECURE_SETTING -> throw new IllegalArgumentException("encrypted_data type [secure_setting] is not supported yet");
            case TYPE_PASSWORD -> new SnapshotEncryptedData(
                TYPE_PASSWORD,
                password != null ? new SecureString(password.toCharArray()) : null,
                passwordId
            );
            default -> throw new IllegalArgumentException("unknown encrypted_data.type [" + type + "]");
        };
    }

    private static String stringValue(Map.Entry<String, Object> entry) {
        if (entry.getValue() instanceof String value) {
            return value;
        }
        throw new IllegalArgumentException("encrypted_data." + entry.getKey() + " must be a string");
    }

    /**
     * Validates that this instance is self-consistent. Throws {@link IllegalArgumentException} if any
     * type-specific required field is missing or invalid.
     */
    public void validate() {
        if (passwordId != null && PASSWORD_ID_PATTERN.matcher(passwordId).matches() == false) {
            throw new IllegalArgumentException("encrypted_data.password_id must be 1 to 128 characters from [A-Za-z0-9_-]");
        }
        switch (type) {
            case TYPE_PASSWORD -> {
                if (password == null) {
                    throw new IllegalArgumentException("encrypted_data.password is required for type [password]");
                }
                validatePassword(password);
            }
            case TYPE_SECURE_SETTING -> throw new IllegalArgumentException("encrypted_data type [secure_setting] is not supported yet");
            default -> throw new IllegalArgumentException("unknown encrypted_data.type [" + type + "]");
        }
    }

    private static void validatePassword(SecureString password) {
        if (password.length() < MIN_PASSWORD_LENGTH) {
            throw new IllegalArgumentException("encrypted_data.password must be at least " + MIN_PASSWORD_LENGTH + " characters");
        }
        if (password.length() > MAX_PASSWORD_LENGTH) {
            throw new IllegalArgumentException("encrypted_data.password must be at most " + MAX_PASSWORD_LENGTH + " characters");
        }
        if (REDACTED_SENTINEL.contentEquals(password)) {
            throw new IllegalArgumentException("encrypted_data.password must not be the redaction sentinel");
        }
        if (TEMPLATE_PATTERNS.keySet().stream().anyMatch(pattern -> pattern.matcher(password).matches())) {
            throw new IllegalArgumentException(
                "encrypted_data.password must not be a bare template variable (e.g. " + TEMPLATE_EXAMPLES + ")"
            );
        }
    }
}
