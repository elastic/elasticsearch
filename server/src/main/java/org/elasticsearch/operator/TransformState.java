/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator;

import org.elasticsearch.cluster.ClusterState;

import java.util.Set;

/**
 * A {@link ClusterState} wrapper used by the OperatorClusterStateController to pass the
 * current state as well as previous keys set by an {@link OperatorHandler} to each transform
 * step of the cluster state update.
 *
 */
public record TransformState(ClusterState state, Set<String> keys) {}
