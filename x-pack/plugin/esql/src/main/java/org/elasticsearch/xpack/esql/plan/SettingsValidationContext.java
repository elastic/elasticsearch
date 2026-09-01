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
 * NOTE for anyone adding a field here: a field that cannot be determined when an operator WRITES a cluster setting
 * must not be readable by any setting declared with withClusterDefault(). Cluster-setting validation runs under a
 * static context built at class initialization (see QuerySettingDef), where only build-type-like facts are truthful;
 * crossProjectEnabled is already a placeholder there. The guard is structural but indirect — build() refuses
 * withClusterDefault() on a serverlessOnly setting, which today is exactly the set whose validators read the
 * placeholder. If you add a field in that category, extend that guard rather than relying on the correlation holding.
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
