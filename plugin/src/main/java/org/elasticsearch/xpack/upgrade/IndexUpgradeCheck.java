/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Interface that for an index check and upgrade process
 */
public interface IndexUpgradeCheck {
    String getName();

    UpgradeActionRequired actionRequired(IndexMetaData indexMetaData, Map<String, String> params, ClusterState state);

    default Collection<String> supportedParams() {
        return Collections.emptyList();
    }
}
