/*
 * @notice
 * Copyright 2011-2019 the original author or authors.
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is derived from the nebula gradle-ospackage-plugin
 * (https://github.com/nebula-plugins/gradle-ospackage-plugin), reduced to the
 * functionality used by the Elasticsearch build and rewritten in Java.
 */

package org.elasticsearch.gradle.internal.ospackage.rpm;

import org.elasticsearch.gradle.internal.ospackage.PackagingUtils;
import org.gradle.api.file.FileCopyDetails;
import org.redline_rpm.Builder;
import org.redline_rpm.payload.Directive;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Adds visited files, directories and symlinks to the redline rpm {@link Builder}, translating
 * symbolic links in the copied tree into rpm link entries.
 */
class RpmFileVisitorStrategy {

    private final Builder builder;
    private final Set<PackagingUtils.Symlink> links = new LinkedHashSet<>();

    RpmFileVisitorStrategy(Builder builder) {
        this.builder = builder;
    }

    void addFile(
        FileCopyDetails details,
        File source,
        int mode,
        int dirmode,
        Directive directive,
        String uname,
        String gname,
        boolean addParents
    ) {
        try {
            File file = details.getFile();
            File parentLink = PackagingUtils.parentSymbolicLink(file);
            if (parentLink != null) {
                PackagingUtils.Symlink link = PackagingUtils.relativizeSymlink(details, parentLink);
                if (link != null) {
                    addLinkToBuilder(link);
                    return;
                }
            } else if (PackagingUtils.isSymbolicLink(file)) {
                PackagingUtils.Symlink link = PackagingUtils.relativizeSymlink(details, file);
                if (link != null) {
                    addLinkToBuilder(link);
                    return;
                }
            }
            addFileToBuilder(details, source, mode, dirmode, directive, uname, gname, addParents);
        } catch (UnsupportedOperationException e) {
            // for filtered file details accessing the file throws this exception
            addFileToBuilder(details, source, mode, dirmode, directive, uname, gname, addParents);
        }
    }

    void addDirectory(FileCopyDetails details, int permissions, Directive directive, String uname, String gname, boolean addParents) {
        try {
            File file = details.getFile();
            if (PackagingUtils.isSymbolicLink(file)) {
                PackagingUtils.Symlink link = PackagingUtils.relativizeSymlink(details, file);
                if (link != null) {
                    addLinkToBuilder(link);
                    return;
                }
            }
            if (PackagingUtils.parentSymbolicLink(file) == null) {
                addDirectoryToBuilder(details, permissions, directive, uname, gname, addParents);
            }
        } catch (UnsupportedOperationException e) {
            // for filtered file details accessing the directory throws this exception
            addDirectoryToBuilder(details, permissions, directive, uname, gname, addParents);
        }
    }

    private void addFileToBuilder(
        FileCopyDetails details,
        File source,
        int mode,
        int dirmode,
        Directive directive,
        String uname,
        String gname,
        boolean addParents
    ) {
        try {
            builder.addFile(PackagingUtils.getRootPath(details), source, mode, dirmode, directive, uname, gname, addParents);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private void addDirectoryToBuilder(
        FileCopyDetails details,
        int permissions,
        Directive directive,
        String uname,
        String gname,
        boolean addParents
    ) {
        try {
            builder.addDirectory(PackagingUtils.getRootPath(details), permissions, directive, uname, gname, addParents);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void addLinkToBuilder(PackagingUtils.Symlink link) {
        if (links.add(link)) {
            try {
                builder.addLink(link.path(), link.target());
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
