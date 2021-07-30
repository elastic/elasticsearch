/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.watcher;

import java.nio.file.Path;

/**
 * Callback interface that file changes File Watcher is using to notify listeners about changes.
 */
public interface FileChangesListener {
    /**
     * Called for every file found in the watched directory during initialization
     */
    default void onFileInit(Path file) {}

    /**
     * Called for every subdirectory found in the watched directory during initialization
     */
    default void onDirectoryInit(Path file) {}

    /**
     * Called for every new file found in the watched directory
     */
    default void onFileCreated(Path file) {}

    /**
     * Called for every file that disappeared in the watched directory
     */
    default void onFileDeleted(Path file) {}

    /**
     * Called for every file that was changed in the watched directory
     */
    default void onFileChanged(Path file) {}

    /**
     * Called for every new subdirectory found in the watched directory
     */
    default void onDirectoryCreated(Path file) {}

    /**
     * Called for every file that disappeared in the watched directory
     */
    default void onDirectoryDeleted(Path file) {}
}
