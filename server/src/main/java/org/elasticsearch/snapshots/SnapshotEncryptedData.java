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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The {@code encrypted_data} block from a create-snapshot request. Carries a type discriminator
 * ({@value #TYPE_PASSWORD} or {@value #TYPE_SECURE_SETTING}) and type-specific fields. Only
 * {@code type: password} is executable today; {@code type: secure_setting} is reserved for a
 * follow-up implementation.
 *
 * <p>Instances are master-local: the password must never travel in cluster state.
 */
public final class SnapshotEncryptedData implements Writeable {

    /**
     * Wire-format transport version that added {@code encrypted_data} to
     * {@link org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest}
     * and to {@link SnapshotInfo}.
     */
    public static final TransportVersion TRANSPORT_VERSION = TransportVersion.fromName("snapshot_encrypted_data");

    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("snapshot_encrypted_data");

    public static final String TYPE_PASSWORD = "password";
    public static final String TYPE_SECURE_SETTING = "secure_setting";

    /** Sentinel value rejected by {@link #validatePassword}. */
    static final String REDACTED_SENTINEL = "::es_redacted::";

    private static final int MIN_PASSWORD_LENGTH = 15;

    // Reject passwords that are *entirely* a template fragment (anchored). A password merely
    // containing one of these patterns is still accepted.
    private static final Map<Pattern, String> TEMPLATE_PATTERNS = Map.of(
        Pattern.compile("^\\s*\\$\\{[^}]+\\}\\s*$"),
        "${VAR}",
        Pattern.compile("^\\s*\\$[A-Za-z_][A-Za-z0-9_]*\\s*$"),
        "$VAR",
        Pattern.compile("^\\s*\\{\\{[^}]+\\}\\}\\s*$"),
        "{{ .var }}",
        Pattern.compile("^\\s*%[A-Za-z_][A-Za-z0-9_]*%\\s*$"),
        "%VAR%"
    );
    private static final String TEMPLATE_EXAMPLES = TEMPLATE_PATTERNS.values().stream().sorted().collect(Collectors.joining(", "));

    private final String type;

    @Nullable
    private final SecureString password;

    @Nullable
    private final String passwordId;

    public SnapshotEncryptedData(String type, @Nullable SecureString password, @Nullable String passwordId) {
        this.type = type;
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

    /**
     * Parses an {@code encrypted_data} map value (the object that was the value of the
     * {@code "encrypted_data"} key in the request body). Throws {@link IllegalArgumentException}
     * for malformed input, missing required fields, or unsupported types.
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
                case "type" -> type = String.valueOf(entry.getValue());
                case "password" -> {
                    if (entry.getValue() == null) {
                        throw new IllegalArgumentException("encrypted_data.password must not be null");
                    }
                    password = String.valueOf(entry.getValue());
                }
                case "password_id" -> passwordId = entry.getValue() != null ? String.valueOf(entry.getValue()) : null;
                default -> throw new IllegalArgumentException("unknown field [encrypted_data." + entry.getKey() + "]");
            }
        }
        if (type == null) {
            throw new IllegalArgumentException("encrypted_data.type is required");
        }
        return switch (type) {
            case TYPE_SECURE_SETTING -> throw new IllegalArgumentException("encrypted_data type [secure_setting] is not supported");
            case TYPE_PASSWORD -> new SnapshotEncryptedData(
                TYPE_PASSWORD,
                password != null ? new SecureString(password.toCharArray()) : null,
                passwordId
            );
            default -> throw new IllegalArgumentException("unknown encrypted_data.type [" + type + "]");
        };
    }

    /**
     * Validates {@code password} for use as a snapshot encryption password
     */
    public static void validatePassword(SecureString password) {
        if (password == null || password.length() < MIN_PASSWORD_LENGTH) {
            throw new IllegalArgumentException("encrypted_data.password must be at least " + MIN_PASSWORD_LENGTH + " characters");
        }
        String p = password.toString();
        if (p.equals(REDACTED_SENTINEL)) {
            throw new IllegalArgumentException("encrypted_data.password must not be the redaction sentinel");
        }
        if (TEMPLATE_PATTERNS.keySet().stream().anyMatch(pattern -> pattern.matcher(p).matches())) {
            throw new IllegalArgumentException(
                "encrypted_data.password must not be a bare template variable (e.g. " + TEMPLATE_EXAMPLES + ")"
            );
        }
    }
}
