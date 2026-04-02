/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.watcher.FileWatcher;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

public class PrivilegedFileWatcher extends FileWatcher {

    public PrivilegedFileWatcher(Path path) {
        super(path);
    }

    public PrivilegedFileWatcher(Path path, boolean checkFileContents) {
        super(path, checkFileContents);
    }

    @Override
    protected InputStream newInputStream(Path path) throws IOException {
        return Files.newInputStream(path);
    }

    @Override
    protected boolean fileExists(Path path) {
        return Files.exists(path);
    }

    @Override
    protected BasicFileAttributes readAttributes(Path path) throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class);
    }

    @Override
    protected DirectoryStream<Path> listFiles(Path path) throws IOException {
        return Files.newDirectoryStream(path);
    }
}
