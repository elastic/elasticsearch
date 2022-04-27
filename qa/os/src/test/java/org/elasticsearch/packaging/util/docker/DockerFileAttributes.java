/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util.docker;

import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

class DockerFileAttributes implements PosixFileAttributes {
    String owner;
    String group;
    Set<PosixFilePermission> permissions;
    boolean isDirectory;
    boolean isSymbolicLink;

    @Override
    public UserPrincipal owner() {
        return () -> owner;
    }

    @Override
    public GroupPrincipal group() {
        return () -> group;
    }

    @Override
    public Set<PosixFilePermission> permissions() {
        return permissions;
    }

    @Override
    public FileTime lastModifiedTime() {
        return null;
    }

    @Override
    public FileTime lastAccessTime() {
        return null;
    }

    @Override
    public FileTime creationTime() {
        return null;
    }

    @Override
    public boolean isRegularFile() {
        return isDirectory == false && isSymbolicLink == false;
    }

    @Override
    public boolean isDirectory() {
        return isDirectory;
    }

    @Override
    public boolean isSymbolicLink() {
        return isDirectory;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public Object fileKey() {
        return null;
    }
}
