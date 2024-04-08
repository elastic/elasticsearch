/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Objects;
import java.util.Set;

import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.elasticsearch.packaging.util.FileUtils.getBasicFileAttributes;
import static org.elasticsearch.packaging.util.FileUtils.getFileOwner;
import static org.elasticsearch.packaging.util.FileUtils.getPosixFileAttributes;

/**
 * Asserts that a file at a path matches its status as Directory/File, and its owner. If on a posix system, also matches the permission
 * set is what we expect.
 *
 * This class saves information about its failed matches in instance variables and so instances should not be reused
 */
public class FileMatcher extends TypeSafeMatcher<Path> {

    public enum Fileness {
        File,
        Directory
    }

    public static final Set<PosixFilePermission> p444 = fromString("r--r--r--");
    public static final Set<PosixFilePermission> p555 = fromString("r-xr-xr-x");
    public static final Set<PosixFilePermission> p600 = fromString("rw-------");
    public static final Set<PosixFilePermission> p644 = fromString("rw-r--r--");
    public static final Set<PosixFilePermission> p660 = fromString("rw-rw----");
    public static final Set<PosixFilePermission> p664 = fromString("rw-rw-r--");
    public static final Set<PosixFilePermission> p750 = fromString("rwxr-x---");
    public static final Set<PosixFilePermission> p755 = fromString("rwxr-xr-x");
    public static final Set<PosixFilePermission> p770 = fromString("rwxrwx---");
    public static final Set<PosixFilePermission> p775 = fromString("rwxrwxr-x");

    protected final Fileness fileness;
    protected final String owner;
    protected final String group;
    protected final Set<PosixFilePermission> posixPermissions;

    protected String mismatch;

    public FileMatcher(Fileness fileness, String owner, String group, Set<PosixFilePermission> posixPermissions) {
        this.fileness = Objects.requireNonNull(fileness);
        this.owner = owner;
        this.group = group;
        this.posixPermissions = posixPermissions;
    }

    @Override
    protected boolean matchesSafely(Path path) {
        if (Files.exists(path) == false) {
            mismatch = "Does not exist";
            return false;
        }

        if (Platforms.WINDOWS) {
            final BasicFileAttributes attributes = getBasicFileAttributes(path);

            if (fileness.equals(Fileness.Directory) != attributes.isDirectory()) {
                mismatch = "Is " + (attributes.isDirectory() ? "a directory" : "a file");
                return false;
            }

            if (owner != null) {
                final String attributeViewOwner = getFileOwner(path);
                if (attributeViewOwner.contains(owner) == false) {
                    mismatch = "Owned by " + attributeViewOwner;
                    return false;
                }
            }
        } else {
            final PosixFileAttributes attributes = getPosixFileAttributes(path);

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
        }

        return true;
    }

    @Override
    public void describeMismatchSafely(Path path, Description description) {
        description.appendText("path ").appendValue(path);
        if (mismatch != null) {
            description.appendText(mismatch);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue("file/directory: ")
            .appendValue(fileness)
            .appendText(" with owner ")
            .appendValue(owner)
            .appendText(" with group ")
            .appendValue(group)
            .appendText(" with posix permissions ")
            .appendValueList("[", ",", "]", posixPermissions);
    }

    public static FileMatcher file(Fileness fileness, String owner) {
        return file(fileness, owner, null, null);
    }

    public static FileMatcher file(Fileness fileness, String owner, String group, Set<PosixFilePermission> permissions) {
        return new FileMatcher(fileness, owner, group, permissions);
    }
}
