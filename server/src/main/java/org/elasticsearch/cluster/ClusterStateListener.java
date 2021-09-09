/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

/**
 * A listener to be notified when a cluster state changes.
 *
 *
 */
public interface ClusterStateListener {

    /**
     * Called when cluster state changes.
     */
    void clusterChanged(ClusterChangedEvent event);
}
