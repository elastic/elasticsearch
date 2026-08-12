/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * The {@code encrypted_data} object of the snapshot create and restore APIs: configures how PEK-encrypted cluster
 * state data is protected in a snapshot, and its presence on a request opts that data in. The {@code type}
 * discriminator allows other protection mechanisms (e.g. an external KMS) to be added later without breaking API
 * changes; only {@link #TYPE_PASSWORD} exists today.
 */
public final class SnapshotEncryptedData implements Writeable {

    public static final String TYPE_PASSWORD = "password";

    private final String type;
    private final SecureString password;
    @Nullable
    private final String passwordId;

    /**
     * @param password   the password protecting the encrypted data
     * @param passwordId an optional opaque label for the password, recorded in the snapshot metadata so the password
     *                   can be looked up in an external store at restore time; never a secret
     */
    public SnapshotEncryptedData(SecureString password, @Nullable String passwordId) {
        this.type = TYPE_PASSWORD;
        this.password = Objects.requireNonNull(password, "encrypted_data.password is required");
        this.passwordId = passwordId;
    }

    public SnapshotEncryptedData(StreamInput in) throws IOException {
        this.type = in.readString();
        this.password = in.readSecureString();
        this.passwordId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeSecureString(password);
        out.writeOptionalString(passwordId);
    }

    /**
     * Parses the {@code encrypted_data} object of a snapshot create or restore request body.
     */
    public static SnapshotEncryptedData fromMap(Object value) {
        if (value instanceof Map<?, ?> map == false) {
            throw new IllegalArgumentException("malformed encrypted_data, should be an object");
        }
        String type = null;
        String password = null;
        String passwordId = null;
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            switch (String.valueOf(entry.getKey())) {
                case "type" -> type = requireString(entry, "encrypted_data.type");
                case "password" -> password = requireString(entry, "encrypted_data.password");
                case "password_id" -> passwordId = requireString(entry, "encrypted_data.password_id");
                default -> throw new IllegalArgumentException("unknown encrypted_data field [" + entry.getKey() + "]");
            }
        }
        if (type == null) {
            throw new IllegalArgumentException("encrypted_data.type is required");
        }
        if (TYPE_PASSWORD.equals(type) == false) {
            throw new IllegalArgumentException("unknown encrypted_data.type [" + type + "], only [password] is supported");
        }
        if (password == null) {
            throw new IllegalArgumentException("encrypted_data.password is required");
        }
        return new SnapshotEncryptedData(new SecureString(password.toCharArray()), passwordId);
    }

    private static String requireString(Map.Entry<?, ?> entry, String name) {
        if (entry.getValue() instanceof String s) {
            return s;
        }
        throw new IllegalArgumentException("malformed " + name + ", should be a string");
    }

    public String type() {
        return type;
    }

    public SecureString password() {
        return password;
    }

    @Nullable
    public String passwordId() {
        return passwordId;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SnapshotEncryptedData that
            && type.equals(that.type)
            && password.equals(that.password)
            && Objects.equals(passwordId, that.passwordId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, password, passwordId);
    }
}
