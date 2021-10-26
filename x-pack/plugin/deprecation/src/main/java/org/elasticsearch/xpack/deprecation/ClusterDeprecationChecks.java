/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

public class ClusterDeprecationChecks {
    static DeprecationIssue checkTransientSettingsExistence(ClusterState state) {
        if (state.metadata().transientSettings().isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Transient cluster settings are deprecated",
                "https://ela.st/es-deprecation-7-transient-cluster-settings",
                "Use persistent settings to configure your cluster.",
                false,
                null);
        }
        return null;
    }
}
