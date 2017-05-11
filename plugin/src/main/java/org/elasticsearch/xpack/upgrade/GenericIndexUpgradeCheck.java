/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Map;

/**
 * Generic upgrade check applicable to all indices to be upgraded from the current version
 * to the next major version
 */
public class GenericIndexUpgradeCheck implements IndexUpgradeCheck {
    @Override
    public String getName() {
        return "generic";
    }

    @Override
    public UpgradeActionRequired actionRequired(IndexMetaData indexMetaData, Map<String, String> params, ClusterState state) {
        if (indexMetaData.getCreationVersion().before(Version.V_5_0_0_alpha1)) {
            return UpgradeActionRequired.REINDEX;
        }
        return UpgradeActionRequired.UP_TO_DATE;
    }
}
