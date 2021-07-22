/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.core.TimeValue;

/**
 * An exception to cluster state listener that allows for timeouts and for post added notifications.
 *
 *
 */
public interface TimeoutClusterStateListener extends ClusterStateListener {

    void postAdded();

    void onClose();

    void onTimeout(TimeValue timeout);
}
