/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;

import java.io.Closeable;
import java.io.IOException;

public class PTaskShutdownService implements Closeable, ClusterStateListener {

    private final ClusterService clusterService;

    public PTaskShutdownService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public void init() {
        clusterService.addListener(this);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

    }
}
