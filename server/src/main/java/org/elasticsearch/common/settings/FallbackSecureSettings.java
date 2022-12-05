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
 * WARNING: this is a temporary class to be used only in the context of Stateless. It applies only to a predetermined set of secure
 * settings mainly for repository settings. Other secure settings do not fall back to YML settings.
 */
public class FallbackSecureSettings implements SecureSettings {
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
            .filter(
                key -> ((key.startsWith("s3.client.") && key.endsWith(".access_key"))
                    || (key.startsWith("s3.client.") && key.endsWith(".secret_key"))
                    || (key.startsWith("s3.client.") && key.endsWith(".session_token"))
                    || (key.startsWith("s3.client.") && key.endsWith(".proxy.username"))
                    || (key.startsWith("s3.client.") && key.endsWith(".proxy.password"))
                    || (key.startsWith("gcs.client.") && key.endsWith(".credentials_file"))
                    || (key.startsWith("azure.client.") && key.endsWith(".account"))
                    || (key.startsWith("azure.client.") && key.endsWith(".key"))
                    || (key.startsWith("azure.client.") && key.endsWith(".sas_token")))
            );
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
        return new SecureString(settings.get(setting).toCharArray());
    }

    @Override
    public InputStream getFile(String setting) throws GeneralSecurityException {
        if (secureSettings != null) {
            if (secureSettings.getSettingNames().contains(setting)) {
                return secureSettings.getFile(setting);
            }
        }
        return new ByteArrayInputStream(settings.get(setting).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public byte[] getSHA256Digest(String setting) throws GeneralSecurityException {
        if (secureSettings != null) {
            if (secureSettings.getSettingNames().contains(setting)) {
                return secureSettings.getSHA256Digest(setting);
            }
        }
        return MessageDigests.sha256().digest(settings.get(setting).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        if (secureSettings != null) {
            secureSettings.close();
        }
    }

    public static Settings installFallbackSecureSettings(Settings settings) {
        FallbackSecureSettings fallbackSecureSettings = new FallbackSecureSettings(settings);
        Settings.Builder newSettings = Settings.builder().put(settings, false);
        fallbackSecureSettings.getSettingNames().forEach(key -> { if (newSettings.keys().contains(key)) newSettings.remove(key); });
        newSettings.setSecureSettings(fallbackSecureSettings);
        return newSettings.build();
    }
}
