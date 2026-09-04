/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.List;

/**
 * Node-scope settings for the workload-identity-issuer client.
 *
 * <p>SSL material is configured under the {@code workload_identity.ssl.*} prefix via
 * {@link WorkloadIdentitySslConfig}; HTTP transport tuning lives under
 * {@code workload_identity.http.*} in {@link WorkloadIdentityHttpSettings}.
 */
public final class WorkloadIdentityIssuerSettings {

    /**
     * Setting prefix used by all workload-identity client settings (SSL, HTTP, endpoint).
     */
    public static final String SETTING_PREFIX = "workload_identity.";

    /**
     * Base URL of the workload-identity-issuer internal token endpoint. Must be {@code https}.
     * Token requests are issued as {@code POST <base-url>/token}. Elasticsearch treats the URL
     * as opaque; the issuer derives any deployment-specific routing from the SNI hostname.
     */
    public static final Setting<String> ISSUER_URL_SETTING = Setting.simpleString(SETTING_PREFIX + "issuer.url", Property.NodeScope);

    /**
     * Skew applied when deciding whether a cached JWT is still fresh enough to hand out. A token
     * is evicted from the cache (and the next caller triggers a refresh) at {@code expiresAt -
     * refresh_before_expiry}. Set to absorb expected clock skew between this node, the issuer,
     * and the downstream cloud-provider STS endpoint plus the longest plausible time between
     * minting and use.
     */
    public static final Setting<TimeValue> TOKEN_CACHE_REFRESH_BEFORE_EXPIRY = Setting.timeSetting(
        SETTING_PREFIX + "token_cache.refresh_before_expiry",
        TimeValue.timeValueMinutes(1),
        TimeValue.ZERO,
        Property.NodeScope
    );

    private WorkloadIdentityIssuerSettings() {}

    /**
     * Activation predicate for the workload-identity feature on this node. The module itself is
     * always loaded, but the issuer client only reaches over the network when {@link #ISSUER_URL_SETTING}
     * is configured with a non-blank value. This is the single signal consumers should use to
     * determine if workload-identity is available on this cluster; SSL and HTTP settings are
     * configuration of an already-active client. A blank or whitespace-only value is treated as
     * unset so that an unusable URL takes the "feature off" branch rather than failing at node
     * startup.
     *
     * @return {@code true} when {@link #ISSUER_URL_SETTING} is set to a non-blank value on this node.
     */
    public static boolean isEnabled(Settings settings) {
        return Strings.hasText(ISSUER_URL_SETTING.get(settings));
    }

    /**
     * @return the list of node-scope settings registered by the workload-identity module
     *         (the issuer endpoint plus the SSL and HTTP transport settings).
     */
    public static List<Setting<?>> getSettings() {
        return List.of(ISSUER_URL_SETTING, TOKEN_CACHE_REFRESH_BEFORE_EXPIRY);
    }
}
