/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import java.security.BasicPermission;

/**
 * A permission granted to ensure exclusive access to a file.
 * <p>
 * By granting this permission, all code that does not have the same permission on the same file
 * will be denied all read/write access to that file.
 */
public class ExclusiveFileAccessPermission extends BasicPermission {
    public ExclusiveFileAccessPermission(String path) {
        super(path, "");
    }
}
