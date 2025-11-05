/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.bootstrap;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

/**
 * Context passed to bootstrap checks containing environment and metadata information.
 * <p>
 * This record encapsulates the information needed by bootstrap checks to validate the
 * node's configuration. It provides access to the node's environment settings and
 * cluster metadata.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * public BootstrapCheckResult check(BootstrapContext context) {
 *     Settings settings = context.settings();
 *     String nodeName = settings.get("node.name");
 *     // perform validation based on settings
 *     return BootstrapCheckResult.success();
 * }
 * }</pre>
 *
 * @param environment the node's environment containing paths and settings
 * @param metadata the cluster metadata
 */
public record BootstrapContext(Environment environment, Metadata metadata) {

    /**
     * Returns the node's settings from the environment.
     * <p>
     * This is a convenience method equivalent to {@code environment.settings()}.
     *
     * @return the node's settings
     */
    public Settings settings() {
        return environment.settings();
    }
}
