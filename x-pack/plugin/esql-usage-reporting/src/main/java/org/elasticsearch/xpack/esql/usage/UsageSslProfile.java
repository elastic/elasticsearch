/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.core.ssl.extension.SslProfileExtension;

import java.util.Set;

/**
 * Declares the SSL profile {@code xpack.esql.usage_reporting.ssl}, which carries the client certificate for the mutual TLS
 * connection to the Usage API. It is an {@link SslProfileExtension} rather than ad hoc settings so that the configuration is
 * validated at node startup and the profile is reloaded when its files change, with no restart.
 * <p>
 * The profile is kept in a static holder, because this class is created by the extension loader before the plugin can give it
 * anything; the publisher reads {@link #current()} and registers a reload listener on it.
 */
public final class UsageSslProfile implements SslProfileExtension {

    // Static because the extension loader creates this class before the plugin can hand it anything. With several nodes in one
    // JVM (internal test clusters) the last profile applied wins.
    private static volatile SslProfile current;

    /**
     * The profile, or null before x-pack-core has loaded its SSL configurations. It is not null merely because the settings are
     * absent: core applies every extension prefix, with the default trust configuration and no client certificate, so a missing
     * certificate is not detectable from here.
     */
    public static SslProfile current() {
        return current;
    }

    @Override
    public Set<String> getSettingPrefixes() {
        return Set.of(UsageReportingSettings.SSL_PREFIX);
    }

    @Override
    public void applyProfile(String prefix, SslProfile profile) {
        current = profile;
    }
}
