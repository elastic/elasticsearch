/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.Build;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.transport.RemoteClusterService;

/*
 * Adding a field: cluster-setting validation runs under a static context (see QuerySettingDef), where only
 * build-type facts are truthful and crossProjectEnabled is already a placeholder. build() keeps such validators off
 * that path by refusing withClusterDefault() on serverlessOnly settings — today exactly the set that reads it. A new
 * field in that category needs that guard extended, not the correlation trusted.
 */
public record SettingsValidationContext(boolean crossProjectEnabled, boolean isSnapshot) {

    /**
     * Builds a context from a possibly-null {@link RemoteClusterService}. Null is tolerated for callers without a transport
     * service available; cross-project is treated as disabled in that case.
     */
    public static SettingsValidationContext from(RemoteClusterService remoteClusterService) {
        return new SettingsValidationContext(
            remoteClusterService != null && remoteClusterService.crossProjectEnabled(),
            Build.current().isSnapshot()
        );
    }

    /** Builds a context directly from a {@link CrossProjectModeDecider}. */
    public static SettingsValidationContext from(CrossProjectModeDecider decider) {
        return new SettingsValidationContext(decider.crossProjectEnabled(), Build.current().isSnapshot());
    }
}
