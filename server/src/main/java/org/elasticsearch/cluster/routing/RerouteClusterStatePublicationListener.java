/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

/// Listener for the outcome of cluster-state publications produced by [RerouteService].
public interface RerouteClusterStatePublicationListener {

    /// Invoked after a cluster-state update including a reroute is successfully published and processed locally.
    void onSuccessfulPublication(long baseStateTerm, long baseStateVersion);

    /// Invoked if a reroute cluster-state update including a reroute fails before a successful publication.
    void onAbortedPublication(Exception e);
}
