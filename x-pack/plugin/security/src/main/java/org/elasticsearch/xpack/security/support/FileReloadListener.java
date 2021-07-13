/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.watcher.FileChangesListener;

import java.nio.file.Path;

public class FileReloadListener implements FileChangesListener {

    private final Path path;
    private final Runnable reload;

    public FileReloadListener(Path path, Runnable reload) {
        this.path = path;
        this.reload = reload;
    }

    @Override
    public void onFileCreated(Path file) {
        onFileChanged(file);
    }

    @Override
    public void onFileDeleted(Path file) {
        onFileChanged(file);
    }

    @Override
    public void onFileChanged(Path file) {
        if (file.equals(this.path)) {
            reload.run();
        }
    }
}
