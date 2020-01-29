/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    public enum Fileness { File, Directory }

    public static final Set<PosixFilePermission> p775 = fromString("rwxrwxr-x");
    public static final Set<PosixFilePermission> p770 = fromString("rwxrwx---");
    public static final Set<PosixFilePermission> p755 = fromString("rwxr-xr-x");
    public static final Set<PosixFilePermission> p750 = fromString("rwxr-x---");
    public static final Set<PosixFilePermission> p660 = fromString("rw-rw----");
    public static final Set<PosixFilePermission> p644 = fromString("rw-r--r--");
    public static final Set<PosixFilePermission> p600 = fromString("rw-------");

    private final Fileness fileness;
    private final String owner;
    private final String group;
    private final Set<PosixFilePermission> posixPermissions;

    private String mismatch;

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
        description.appendValue("file/directory: ").appendValue(fileness)
            .appendText(" with owner ").appendValue(owner)
            .appendText(" with group ").appendValue(group)
            .appendText(" with posix permissions ").appendValueList("[", ",", "]", posixPermissions);
    }

    public static FileMatcher file(Fileness fileness, String owner) {
        return file(fileness, owner, null, null);
    }

    public static FileMatcher file(Fileness fileness, String owner, String group, Set<PosixFilePermission> permissions) {
        return new FileMatcher(fileness, owner, group, permissions);
    }
}
