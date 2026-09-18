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

/**
 * A subclass of {@link FileWatcher} that lives in the security module.
 * <p>
 * The Elasticsearch entitlement system uses a {@link java.lang.StackWalker} to determine
 * which module is making a file-system call. {@link FileWatcher} lives in the server module
 * and therefore cannot access paths that are marked {@code exclusive: true} for the security
 * module (e.g. {@code users} and {@code x-pack/users}).
 * <p>
 * By overriding every protected file-I/O method here, the actual {@link Files} calls are
 * issued from inside {@code org.elasticsearch.security}, so the entitlement check correctly
 * attributes them to this module.
 */
public class EntitledFileWatcher extends FileWatcher {

    public EntitledFileWatcher(Path path) {
        super(path);
    }

    public EntitledFileWatcher(Path path, boolean checkFileContents) {
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
