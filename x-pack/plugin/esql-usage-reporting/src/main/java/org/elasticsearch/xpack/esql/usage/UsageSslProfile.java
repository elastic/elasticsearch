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
 * It keeps no state, so that nothing of this plugin is JVM-global. The plugin resolves the profile from the SSL service of x-pack-core
 * ({@link EsqlUsageReportingPlugin#getSslService()}): in production that is the shared one, as for the other x-pack plugins; tests
 * that run several nodes in one JVM override that method per node.
 */
public final class UsageSslProfile implements SslProfileExtension {

    @Override
    public Set<String> getSettingPrefixes() {
        return Set.of(UsageReportingSettings.SSL_PREFIX);
    }

    @Override
    public void applyProfile(String prefix, SslProfile profile) {
        // Nothing to do: see the class javadoc.
    }
}
