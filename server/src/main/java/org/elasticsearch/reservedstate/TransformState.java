/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate;

import org.elasticsearch.cluster.ClusterState;

import java.util.Set;

/**
 * A {@link org.elasticsearch.cluster.ClusterState} wrapper used by the ReservedClusterStateService to pass the
 * current state as well as previous keys set by an {@link ReservedClusterStateHandler} to each transform
 * step of the cluster state update.
 */
public record TransformState(ClusterState state, Set<String> keys) {}
