/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.gradle.api.tasks.WorkResult;

/**
 * An interface for tasks that support basic file operations.
 * Methods will be added as needed.
 */
public interface FileSystemOperationsAware {
    WorkResult delete(Object... objects);
}
