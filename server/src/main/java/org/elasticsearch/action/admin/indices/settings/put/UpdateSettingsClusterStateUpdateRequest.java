/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;

import java.util.Objects;

/**
 * Cluster state update request that allows to update settings for some indices
 */
public record UpdateSettingsClusterStateUpdateRequest(
    ProjectId projectId,
    TimeValue masterNodeTimeout,
    TimeValue ackTimeout,
    Settings settings,
    OnExisting onExisting,
    OnStaticSetting onStaticSetting,
    Index... indices
) {

    /**
     * Specifies the behaviour of an update-settings action on existing settings.
     */
    public enum OnExisting {
        /**
         * Update all the specified settings, overwriting any settings which already exist. This is the API default.
         */
        OVERWRITE,

        /**
         * Only add new settings, preserving the values of any settings which are already set and ignoring the new values specified in the
         * request.
         */
        PRESERVE
    }

    /**
     * Specifies the behaviour of an update-settings action which is trying to adjust a non-dynamic setting.
     */
    public enum OnStaticSetting {
        /**
         * Reject attempts to update non-dynamic settings on open indices. This is the API default.
         */
        REJECT,

        /**
         * Automatically close and reopen the shards of any open indices when updating a non-dynamic setting, forcing the shard to
         * reinitialize from scratch.
         */
        REOPEN_INDICES
    }

    public UpdateSettingsClusterStateUpdateRequest {
        Objects.requireNonNull(projectId);
        Objects.requireNonNull(masterNodeTimeout);
        Objects.requireNonNull(ackTimeout);
        Objects.requireNonNull(settings);
        Objects.requireNonNull(indices);
    }
}
