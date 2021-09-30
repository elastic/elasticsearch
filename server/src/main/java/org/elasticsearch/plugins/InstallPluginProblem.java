/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

/**
 * Categories the potential problems that can occur in `InstallPluginAction#execute(List)`. Useful
 * for generating an exit code.
 */
public enum InstallPluginProblem {
    DUPLICATE_PLUGIN_ID,
    NO_XPACK,
    UNKNOWN_PLUGIN,
    RELEASE_SNAPSHOT_MISMATCH,
    MISSING_CHECKSUM,
    INVALID_CHECKSUM,
    PLUGIN_MALFORMED,
    PLUGIN_IS_MODULE,
    PLUGIN_EXISTS,
    INCOMPATIBLE_LICENSE,
    INVALID_SIGNATURE,
    INSTALLATION_FAILED
}
