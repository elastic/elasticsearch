/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlag;

import static org.elasticsearch.common.settings.Setting.boolSetting;

/**
 * Tracks whether the search boost profile is active. The underlying cluster setting
 * ({@code search.boost_profile.enabled}) is dynamic and {@code NodeScope}, so a single instance of
 * this class subscribes to updates via {@link ClusterSettings#initializeAndWatch} and exposes the
 * current value through {@link #isEnabled()}.
 *
 * <p>This class is the single source of truth for both the setting and the feature flag gate, for
 * the boost profile as a whole rather than for any one part of it. Inject it wherever the boost
 * profile eligibility check is needed rather than reading the setting directly.
 */
public final class BoostProfileEnabled {

    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("boost_profile");

    public static final Setting<Boolean> BOOST_PROFILE_ENABLED_SETTING = boolSetting("search.boost_profile.enabled", false, value -> {
        if (value && FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException(
                "[search.boost_profile.enabled] can only be enabled when the boost_profile feature flag is enabled"
            );
        }
    }, Setting.Property.Dynamic, Setting.Property.NodeScope);

    private volatile boolean enabled;

    public BoostProfileEnabled(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(BOOST_PROFILE_ENABLED_SETTING, this::setEnabled);
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns {@code true} when both the cluster setting and the feature flag are active.
     * The feature-flag check here is defence-in-depth: a node whose flag is off will never
     * report or act on the boost profile even if the cluster setting was flipped on by a
     * master whose flag is on.
     */
    public boolean isEnabled() {
        return enabled && FEATURE_FLAG.isEnabled();
    }
}
