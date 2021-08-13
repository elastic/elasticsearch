/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.hash.MessageDigests;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A mock implementation of secure settings for tests to use.
 */
public class MockSecureSettings implements SecureSettings {

    private Map<String, String> secureStrings = new HashMap<>();
    private Map<String, byte[]> files = new HashMap<>();
    private Map<String, byte[]> sha256Digests = new HashMap<>();
    private Set<String> settingNames = new HashSet<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MockSecureSettings() {
    }

    private MockSecureSettings(MockSecureSettings source) {
        secureStrings.putAll(source.secureStrings);
        files.putAll(source.files);
        sha256Digests.putAll(source.sha256Digests);
        settingNames.addAll(source.settingNames);
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
        ensureOpen();
        final String s = secureStrings.get(setting);
        if (s == null) {
            return null;
        }
        return new SecureString(s.toCharArray());
    }

    @Override
    public InputStream getFile(String setting) {
        ensureOpen();
        return new ByteArrayInputStream(files.get(setting));
    }

    @Override
    public byte[] getSHA256Digest(String setting) {
        return sha256Digests.get(setting);
    }

    public void setString(String setting, String value) {
        ensureOpen();
        secureStrings.put(setting, value);
        sha256Digests.put(setting, MessageDigests.sha256().digest(value.getBytes(StandardCharsets.UTF_8)));
        settingNames.add(setting);
    }

    public void setFile(String setting, byte[] value) {
        ensureOpen();
        files.put(setting, value);
        sha256Digests.put(setting, MessageDigests.sha256().digest(value));
        settingNames.add(setting);
    }

    /** Merge the given secure settings into this one. */
    public void merge(MockSecureSettings secureSettings) {
        for (String setting : secureSettings.getSettingNames()) {
            if (settingNames.contains(setting)) {
                throw new IllegalArgumentException("Cannot overwrite existing secure setting " + setting);
            }
        }
        settingNames.addAll(secureSettings.settingNames);
        secureStrings.putAll(secureSettings.secureStrings);
        sha256Digests.putAll(secureSettings.sha256Digests);
        files.putAll(secureSettings.files);
    }

    @Override
    public void close() throws IOException {
        closed.set(true);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("secure settings are already closed");
        }
    }

    public SecureSettings clone() {
        ensureOpen();
        return new MockSecureSettings(this);
    }
}
