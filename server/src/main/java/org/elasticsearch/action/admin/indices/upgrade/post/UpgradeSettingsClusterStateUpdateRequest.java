/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.core.Tuple;

import java.util.Map;

/**
 * Cluster state update request that allows to change minimum compatibility settings for some indices
 */
public class UpgradeSettingsClusterStateUpdateRequest extends ClusterStateUpdateRequest<UpgradeSettingsClusterStateUpdateRequest> {

    private Map<String, Tuple<Version, String>> versions;

    public UpgradeSettingsClusterStateUpdateRequest() {

    }

    /**
     * Returns the index to version map for indices that should be updated
     */
    public Map<String, Tuple<Version, String>> versions() {
        return versions;
    }

    /**
     * Sets the index to version map for indices that should be updated
     */
    public UpgradeSettingsClusterStateUpdateRequest versions(Map<String, Tuple<Version, String>> versions) {
        this.versions = versions;
        return this;
    }
}
