/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;
import java.util.Set;

/**
 * This interface provides methods for retrieving a set of linked project aliases
 * based on the current cluster state. It is intended to abstract the logic for
 * determining the linked project aliases that need to be queried and to remove dependency from
 * autoscaling to code in the cross-project module, avoiding potential circular dependencies.
 */
public interface LinkedProjectAliasesHelper {

    LinkedProjectAliasesHelper NOOP = state -> Collections.emptySet();

    Set<String> getLinkedProjectAliasesToQuery(ClusterState state);
}
