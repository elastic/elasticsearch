/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of {@link SecureSettings} that represents secrets in cluster state. Secrets are stored as byte arrays along with
 * their SHA-256 digests. Provides functionality to read and serialize secure settings to broadcast them as part of cluster state.
 * Does not provide any encryption.
 */
public class SecureClusterStateSettings implements SecureSettings {

    private final Map<String, Entry> secrets;

    public SecureClusterStateSettings(SecureSettings secureSettings) {
        secrets = new HashMap<>();
        for (String key : secureSettings.getSettingNames()) {
            try {
                secrets.put(
                    key,
                    new Entry(getValueAsByteArray(secureSettings, key), SecureClusterStateSettings.getSHA256Digest(secureSettings, key))
                );
            } catch (IOException | GeneralSecurityException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public SecureClusterStateSettings(Map<String, byte[]> settings) {
        secrets = settings.entrySet()
            .stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, entry -> new Entry(entry.getValue(), MessageDigests.sha256().digest(entry.getValue())))
            );
    }

    SecureClusterStateSettings(StreamInput in) throws IOException {
        secrets = in.readMap(StreamInput::readString, v -> new Entry(in.readByteArray(), in.readByteArray()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(secrets, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public Set<String> getSettingNames() {
        return secrets.keySet();
    }

    @Override
    public SecureString getString(String setting) {
        Entry value = secrets.get(setting);
        if (value == null) {
            return null;
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.secret());
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);
        return new SecureString(Arrays.copyOfRange(charBuffer.array(), charBuffer.position(), charBuffer.limit()));
    }

    @Override
    public InputStream getFile(String setting) {
        var value = secrets.get(setting);
        if (value == null) {
            return null;
        }
        return new ByteArrayInputStream(value.secret());
    }

    @Override
    public byte[] getSHA256Digest(String setting) {
        return secrets.get(setting).sha256Digest();
    }

    @Override
    public void close() {
        if (null != secrets && secrets.isEmpty() == false) {
            for (var entry : secrets.entrySet()) {
                entry.setValue(null);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecureClusterStateSettings secrets1 = (SecureClusterStateSettings) o;
        return Objects.equals(secrets, secrets1.secrets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(secrets);
    }

    private static byte[] getValueAsByteArray(SecureSettings secureSettings, String key) throws GeneralSecurityException, IOException {
        return secureSettings.getFile(key).readAllBytes();
    }

    private static byte[] getSHA256Digest(SecureSettings secureSettings, String key) {
        try {
            return secureSettings.getSHA256Digest(key);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    record Entry(byte[] secret, byte[] sha256Digest) implements Writeable {

        Entry(StreamInput in) throws IOException {
            this(in.readByteArray(), in.readByteArray());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry entry = (Entry) o;
            return Arrays.equals(secret, entry.secret) && Arrays.equals(sha256Digest, entry.sha256Digest);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(secret);
            result = 31 * result + Arrays.hashCode(sha256Digest);
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(secret);
            out.writeByteArray(sha256Digest);
        }
    }
}
