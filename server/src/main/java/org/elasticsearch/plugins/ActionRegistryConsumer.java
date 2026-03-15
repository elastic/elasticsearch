/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.plugins;

import org.elasticsearch.action.ActionModule;

/**
 * A lifecycle callback interface for plugins that need access to the
 * fully-built {@link ActionModule} after all actions have been registered.
 * Invoked during node construction, before the node accepts traffic.
 */
public interface ActionRegistryConsumer {

    /**
     * Called once all actions have been registered in the {@link ActionModule}.
     */
    void onActionRegistryReady(ActionModule actionModule);
}
