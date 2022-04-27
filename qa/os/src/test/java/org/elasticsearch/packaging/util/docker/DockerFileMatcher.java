/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util.docker;

import org.elasticsearch.packaging.util.FileMatcher;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

public class DockerFileMatcher extends FileMatcher {

    public DockerFileMatcher(Fileness fileness, String owner, String group, Set<PosixFilePermission> posixPermissions) {
        super(fileness, owner, group, posixPermissions);
    }

    @Override
    protected boolean matchesSafely(Path path) {
        final PosixFileAttributes attributes;
        try {
            attributes = Docker.getAttributes(path);
        } catch (FileNotFoundException e) {
            mismatch = "Does not exist";
            return false;
        }

        if (fileness.equals(Fileness.Directory) != attributes.isDirectory()) {
            mismatch = "Is " + (attributes.isDirectory() ? "a directory" : "a file");
            return false;
        }

        if (owner != null && owner.equals(attributes.owner().getName()) == false) {
            mismatch = "Owned by " + attributes.owner().getName();
            return false;
        }

        if (group != null && group.equals(attributes.group().getName()) == false) {
            mismatch = "Owned by group " + attributes.group().getName();
            return false;
        }

        if (posixPermissions != null && posixPermissions.equals(attributes.permissions()) == false) {
            mismatch = "Has permissions " + attributes.permissions();
            return false;
        }

        return true;
    }

    public static DockerFileMatcher file(Set<PosixFilePermission> permissions) {
        return file(Fileness.File, permissions);
    }

    public static DockerFileMatcher file(Fileness fileness, Set<PosixFilePermission> permissions) {
        return file(fileness, "elasticsearch", "root", permissions);
    }

    public static DockerFileMatcher file(String owner, String group, Set<PosixFilePermission> permissions) {
        return file(Fileness.File, owner, group, permissions);
    }

    public static DockerFileMatcher file(Fileness fileness, String owner, String group, Set<PosixFilePermission> permissions) {
        return new DockerFileMatcher(fileness, owner, group, permissions);
    }
}
