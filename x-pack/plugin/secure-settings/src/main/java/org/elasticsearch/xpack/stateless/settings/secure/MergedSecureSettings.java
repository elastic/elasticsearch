/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CharArrays;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A read-only view over several {@link SecureSettings} sources in order of precedence: a name resolves to the first source that
 * defines it. Names in {@code hidden} resolve to nothing, even if a lower-precedence source defines them.
 * <p>Closing this view doesn't close the sources; they are owned by the caller.
 */
final class MergedSecureSettings implements SecureSettings {

    private final List<SecureSettings> sources;
    private final Set<String> settingNames;

    MergedSecureSettings(List<SecureSettings> sources, Set<String> hidden) {
        this.sources = List.copyOf(sources);
        Set<String> names = new HashSet<>();
        for (SecureSettings source : sources) {
            names.addAll(source.getSettingNames());
        }
        names.removeAll(hidden);
        this.settingNames = Set.copyOf(names);
    }

    private SecureSettings sourceFor(String setting) {
        if (settingNames.contains(setting) == false) {
            return null;
        }
        for (SecureSettings source : sources) {
            if (source.getSettingNames().contains(setting)) {
                return source;
            }
        }
        return null;
    }

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public Set<String> getSettingNames() {
        return settingNames;
    }

    @Override
    public SecureString getString(String setting) {
        SecureSettings source = sourceFor(setting);
        return source == null ? null : source.getString(setting);
    }

    @Override
    public InputStream getFile(String setting) throws GeneralSecurityException {
        SecureSettings source = sourceFor(setting);
        return source == null ? null : source.getFile(setting);
    }

    @Override
    public byte[] getSHA256Digest(String setting) throws GeneralSecurityException {
        SecureSettings source = sourceFor(setting);
        return source == null ? null : source.getSHA256Digest(setting);
    }

    @Override
    public void close() {}

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("merged secure settings are node-local and cannot be serialized");
    }

    /**
     * Secure setting values held in memory: decrypted API-set values for the duration of a reload, or a copy of the keystore.
     */
    static final class InMemory implements SecureSettings {

        static final InMemory EMPTY = new InMemory(Map.of());

        private final Map<String, byte[]> values;

        InMemory(Map<String, byte[]> values) {
            this.values = Map.copyOf(values);
        }

        /**
         * Copies every value of {@code source}, so the copy stays readable after {@code source} is closed.
         */
        static InMemory copyOf(SecureSettings source) throws GeneralSecurityException, IOException {
            Map<String, byte[]> values = new HashMap<>();
            try {
                for (String name : source.getSettingNames()) {
                    try (InputStream file = source.getFile(name)) {
                        values.put(name, file.readAllBytes());
                    }
                }
                return new InMemory(values);
            } catch (GeneralSecurityException | IOException | RuntimeException e) {
                values.values().forEach(value -> Arrays.fill(value, (byte) 0));
                throw e;
            }
        }

        @Override
        public boolean isLoaded() {
            return true;
        }

        @Override
        public Set<String> getSettingNames() {
            return values.keySet();
        }

        @Override
        public SecureString getString(String setting) {
            byte[] value = values.get(setting);
            return value == null ? null : new SecureString(CharArrays.utf8BytesToChars(value));
        }

        @Override
        public InputStream getFile(String setting) {
            byte[] value = values.get(setting);
            return value == null ? null : new ByteArrayInputStream(value);
        }

        @Override
        public byte[] getSHA256Digest(String setting) {
            byte[] value = values.get(setting);
            return value == null ? null : MessageDigests.sha256().digest(value);
        }

        @Override
        public void close() {
            values.values().forEach(value -> Arrays.fill(value, (byte) 0));
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException("decrypted secure settings are node-local and cannot be serialized");
        }
    }
}
