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
 * Extension point for installing a {@link TaskAdmissionPolicy}.
 * Registered via SPI ({@code META-INF/services/}) and loaded by {@code NodeConstruction}
 * using {@code loadSingletonServiceProvider}. When no provider is registered the node
 * uses {@link TaskAdmissionPolicy#NOOP}.
 *
 * <p>Serverless implements this to supply the coordinating-query action names and limit.
 */
@FunctionalInterface
public interface TaskAdmissionPolicyProvider {

    /**
     * Builds the policy to install on this node's {@link TaskManager}.
     */
    TaskAdmissionPolicy create();
}
