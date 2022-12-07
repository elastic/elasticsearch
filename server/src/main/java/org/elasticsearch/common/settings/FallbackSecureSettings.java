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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An implementation of secure settings that fall back to the YML settings.
 *
 * WARNING: this is a temporary class to be used only in the context of Stateless. It applies only to YML settings with a predetermined
 * prefix. Other secure settings do not fall back to YML settings.
 */
public class FallbackSecureSettings implements SecureSettings {
    static final String FALLBACK_PREFIX = "insecure.";
    static final Setting.AffixSetting<String> FALLBACK_SETTINGS = Setting.prefixKeySetting(
        FALLBACK_PREFIX,
        (key) -> Setting.simpleString(key, Setting.Property.NodeScope)
    );

    private final SecureSettings secureSettings;
    private final Settings settings;
    private final Set<String> names;

    private FallbackSecureSettings(Settings settings) {
        if (DiscoveryNode.isStateless(settings) == false) {
            throw new IllegalArgumentException("FallbackSecureSettings are supported only in stateless");
        }

        SecureSettings secureSettings = Settings.builder().put(settings, true).getSecureSettings();
        this.secureSettings = secureSettings;
        this.settings = Settings.builder().put(settings, false).build();

        Stream<String> stream = settings.keySet()
            .stream()
            .filter(key -> (key.startsWith(FALLBACK_PREFIX)))
            .map(s -> s.replace(FALLBACK_PREFIX, ""));
        if (secureSettings != null) {
            stream = Stream.concat(stream, secureSettings.getSettingNames().stream());
        }
        this.names = stream.collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public boolean isLoaded() {
        if (secureSettings != null) {
            return secureSettings.isLoaded();
        }
        return true;
    }

    @Override
    public Set<String> getSettingNames() {
        return names;
    }

    @Override
    public SecureString getString(String setting) throws GeneralSecurityException {
        if (secureSettings != null) {
            if (secureSettings.getSettingNames().contains(setting)) {
                return secureSettings.getString(setting);
            }
        }
        return new SecureString(FALLBACK_SETTINGS.getConcreteSetting(FALLBACK_PREFIX + setting).get(settings).toCharArray());
    }

    @Override
    public InputStream getFile(String setting) throws GeneralSecurityException {
        if (secureSettings != null) {
            if (secureSettings.getSettingNames().contains(setting)) {
                return secureSettings.getFile(setting);
            }
        }
        return new ByteArrayInputStream(
            FALLBACK_SETTINGS.getConcreteSetting(FALLBACK_PREFIX + setting).get(settings).getBytes(StandardCharsets.UTF_8)
        );
    }

    @Override
    public byte[] getSHA256Digest(String setting) throws GeneralSecurityException {
        if (secureSettings != null) {
            if (secureSettings.getSettingNames().contains(setting)) {
                return secureSettings.getSHA256Digest(setting);
            }
        }
        return MessageDigests.sha256()
            .digest(FALLBACK_SETTINGS.getConcreteSetting(FALLBACK_PREFIX + setting).get(settings).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        if (secureSettings != null) {
            secureSettings.close();
        }
    }

    public static Settings installFallbackSecureSettings(Settings settings) {
        FallbackSecureSettings fallbackSecureSettings = new FallbackSecureSettings(settings);
        return Settings.builder().put(settings, false).setSecureSettings(fallbackSecureSettings).build();
    }
}
