/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.security.BasicPermission;

/**
 * A permission plugins can grant themselves to give themselves exclusive access to a file.
 * <p>
 * By granting this permission, all plugins that do not have the same permission on the same file
 * will be denied all read/write access to that file.
 */
public class PluginExclusiveFileAccessPermission extends BasicPermission {
    public PluginExclusiveFileAccessPermission(String path) {
        super(path, "");
    }
}
