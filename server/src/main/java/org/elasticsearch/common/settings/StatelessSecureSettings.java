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
 * An implementation of secure settings that can fall back to the YML settings.
 *
 * WARNING: this is a temporary class to be used only in the context of Stateless. It applies only to YML settings with a predetermined
 * prefix. Other original secure settings are still kept and are accessible (without falling back to YML settings).
 */
public class StatelessSecureSettings implements SecureSettings {
    static final String PREFIX = "insecure.";
    static final Setting.AffixSetting<String> STATELESS_SECURE_SETTINGS = Setting.prefixKeySetting(
        PREFIX,
        (key) -> Setting.simpleString(key, Setting.Property.NodeScope)
    );

    private final SecureSettings secureSettings;
    private final Settings settings;
    private final Set<String> names;

    private StatelessSecureSettings(Settings settings) {
        if (DiscoveryNode.isStateless(settings) == false) {
            throw new IllegalArgumentException("StatelessSecureSettings are supported only in stateless");
        }

        SecureSettings secureSettings = Settings.builder().put(settings, true).getSecureSettings();
        this.secureSettings = secureSettings;
        this.settings = Settings.builder().put(settings, false).build();

        Stream<String> stream = settings.keySet().stream().filter(key -> (key.startsWith(PREFIX))).map(s -> s.replace(PREFIX, ""));
        if (secureSettings != null) {
            stream = Stream.concat(stream, secureSettings.getSettingNames().stream());
        }
        this.names = stream.collect(Collectors.toUnmodifiableSet());
    }

    public static Settings install(Settings settings) {
        StatelessSecureSettings statelessSecureSettings = new StatelessSecureSettings(settings);
        return Settings.builder().put(settings, false).setSecureSettings(statelessSecureSettings).build();
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
        return new SecureString(STATELESS_SECURE_SETTINGS.getConcreteSetting(PREFIX + setting).get(settings).toCharArray());
    }

    @Override
    public InputStream getFile(String setting) throws GeneralSecurityException {
        if (secureSettings != null) {
            if (secureSettings.getSettingNames().contains(setting)) {
                return secureSettings.getFile(setting);
            }
        }
        return new ByteArrayInputStream(
            STATELESS_SECURE_SETTINGS.getConcreteSetting(PREFIX + setting).get(settings).getBytes(StandardCharsets.UTF_8)
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
            .digest(STATELESS_SECURE_SETTINGS.getConcreteSetting(PREFIX + setting).get(settings).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        if (secureSettings != null) {
            secureSettings.close();
        }
    }
}
