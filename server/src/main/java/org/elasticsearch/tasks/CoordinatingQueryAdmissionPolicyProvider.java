/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

/**
 * Extension point for installing this node's {@link CoordinatingQueryAdmissionPolicy}.
 * Registered via SPI ({@code META-INF/services/}) and loaded by {@code NodeConstruction}
 * using {@code loadSingletonServiceProvider}. When no provider is registered the node
 * uses {@link CoordinatingQueryAdmissionPolicy#NOOP}.
 */
@FunctionalInterface
public interface CoordinatingQueryAdmissionPolicyProvider {

    /**
     * Builds the policy bound for {@code _search} and ES|QL on this node.
     * {@code taskManager} is the node's task manager: the policy uses it to walk local
     * parents and to release slots when tasks unregister.
     */
    CoordinatingQueryAdmissionPolicy create(TaskManager taskManager);
}
