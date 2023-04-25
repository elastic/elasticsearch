/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.env.NodeEnvironment;

/**
 * A plugin that performs operations when the node is shut down, i.e. in response to a sigterm.
 */
public interface NodeTerminationHandlerPlugin {

    void handleTerminationSignal(NodeClient client, NodeEnvironment nodeEnvironment);
}
