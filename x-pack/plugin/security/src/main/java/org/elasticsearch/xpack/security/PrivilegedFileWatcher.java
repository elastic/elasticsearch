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
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static java.security.AccessController.doPrivileged;

/**
 * Extension of {@code FileWatcher} that does privileged calls to IO.
 * <p>
 * This class exists so that the calls into the IO methods get here first in the security stackwalk,
 * enabling us to use doPrivileged to ensure we have access. If we don't do this, the code location
 * that is doing the accessing is not the one that is granted the SecuredFileAccessPermission,
 * so the check in ESPolicy fails.
 */
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
        return doPrivileged((PrivilegedAction<Boolean>) () -> Files.exists(path));
    }

    @Override
    protected BasicFileAttributes readAttributes(Path path) throws IOException {
        try {
            return doPrivileged(
                (PrivilegedExceptionAction<BasicFileAttributes>) () -> Files.readAttributes(path, BasicFileAttributes.class)
            );
        } catch (PrivilegedActionException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected DirectoryStream<Path> listFiles(Path path) throws IOException {
        try {
            return doPrivileged((PrivilegedExceptionAction<DirectoryStream<Path>>) () -> Files.newDirectoryStream(path));
        } catch (PrivilegedActionException e) {
            throw new IOException(e);
        }
    }
}
