/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.util.List;

public interface RemovePluginProvider {
    Tuple<RemovePluginProblem, String> checkRemovePlugins(List<PluginDescriptor> plugins) throws IOException;

    void removePlugins(List<PluginDescriptor> plugins) throws Exception;

    void setPurge(boolean purge);
}
