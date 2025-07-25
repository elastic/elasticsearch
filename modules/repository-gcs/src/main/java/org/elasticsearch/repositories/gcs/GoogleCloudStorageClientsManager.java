/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public class GoogleCloudStorageClientsManager implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageClientsManager.class);

    @Override
    public void applyClusterState(ClusterChangedEvent event) {

    }
}
