/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

/**
 * Categories the potential problems that `RemovePluginAction#checkRemovePlugins(List)` can find. Useful
 * for generating an exit code.
 */
public enum RemovePluginProblem {
    NOT_FOUND,
    STILL_USED,
    BIN_FILE_NOT_DIRECTORY
}
