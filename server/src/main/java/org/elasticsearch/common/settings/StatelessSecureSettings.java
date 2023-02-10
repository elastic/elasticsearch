/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation of secure settings from YML settings.
 *
 * WARNING: this is a temporary class only for Stateless. It applies only to YML settings with a predetermined prefix.
 */
public class StatelessSecureSettings implements SecureSettings {
    static final String PREFIX = "insecure.";
    static final Setting.AffixSetting<String> STATELESS_SECURE_SETTINGS = Setting.prefixKeySetting(
        PREFIX,
        (key) -> Setting.simpleString(key, Setting.Property.NodeScope)
    );

    private final Settings settings;
    private final Set<String> names;

    private StatelessSecureSettings(Settings settings) {
        if (DiscoveryNode.isStateless(settings) == false) {
            throw new IllegalArgumentException("StatelessSecureSettings are supported only in stateless");
        }
        this.settings = Settings.builder().put(settings, false).build();
        this.names = settings.keySet()
            .stream()
            .filter(key -> (key.startsWith(PREFIX)))
            .map(s -> s.replace(PREFIX, ""))
            .collect(Collectors.toUnmodifiableSet());
    }

    public static Settings install(Settings settings) {
        StatelessSecureSettings statelessSecureSettings = new StatelessSecureSettings(settings);
        return Settings.builder().put(settings, false).setSecureSettings(statelessSecureSettings).build();
    }

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public Set<String> getSettingNames() {
        return names;
    }

    @Override
    public SecureString getString(String setting) {
        return new SecureString(STATELESS_SECURE_SETTINGS.getConcreteSetting(PREFIX + setting).get(settings).toCharArray());
    }

    @Override
    public InputStream getFile(String setting) throws GeneralSecurityException {
        return new ByteArrayInputStream(
            STATELESS_SECURE_SETTINGS.getConcreteSetting(PREFIX + setting).get(settings).getBytes(StandardCharsets.UTF_8)
        );
    }

    @Override
    public byte[] getSHA256Digest(String setting) {
        return MessageDigests.sha256()
            .digest(STATELESS_SECURE_SETTINGS.getConcreteSetting(PREFIX + setting).get(settings).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new IllegalStateException("Unsupported operation");
    }
}
