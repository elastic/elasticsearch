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

package org.elasticsearch.gradle.internal.ospackage.deb;

import org.elasticsearch.gradle.internal.ospackage.PackagingUtils;
import org.gradle.api.file.FileCopyDetails;
import org.vafer.jdeb.DataProducer;
import org.vafer.jdeb.producers.DataProducerLink;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Adds visited files, directories and symlinks as jdeb {@link DataProducer}s, translating symbolic
 * links in the copied tree into deb link entries.
 */
class DebFileVisitorStrategy {

    private final List<DataProducer> dataProducers;
    private final List<DebCopyAction.InstallDir> installDirs;
    private final Set<PackagingUtils.Symlink> links = new LinkedHashSet<>();

    DebFileVisitorStrategy(List<DataProducer> dataProducers, List<DebCopyAction.InstallDir> installDirs) {
        this.dataProducers = dataProducers;
        this.installDirs = installDirs;
    }

    void addFile(FileCopyDetails details, File source, String user, int uid, String group, int gid, int mode) {
        try {
            File file = details.getFile();
            File parentLink = PackagingUtils.parentSymbolicLink(file);
            if (parentLink != null) {
                PackagingUtils.Symlink link = PackagingUtils.relativizeSymlink(details, parentLink);
                if (link != null) {
                    addProducerLink(link);
                    return;
                }
            } else if (PackagingUtils.isSymbolicLink(file)) {
                PackagingUtils.Symlink link = PackagingUtils.relativizeSymlink(details, file);
                if (link != null) {
                    addProducerLink(link);
                    return;
                }
            }
            addProducerFile(details, source, user, uid, group, gid, mode);
        } catch (UnsupportedOperationException e) {
            // for filtered file details accessing the file throws this exception
            addProducerFile(details, source, user, uid, group, gid, mode);
        }
    }

    void addDirectory(FileCopyDetails details, String user, int uid, String group, int gid, int mode) {
        try {
            File file = details.getFile();
            if (PackagingUtils.isSymbolicLink(file)) {
                PackagingUtils.Symlink link = PackagingUtils.relativizeSymlink(details, file);
                if (link != null) {
                    addProducerLink(link);
                    return;
                }
            }
            if (PackagingUtils.parentSymbolicLink(file) == null) {
                addProducerDirectoryAndInstallDir(details, user, uid, group, gid, mode);
            }
        } catch (UnsupportedOperationException e) {
            // for filtered file details accessing the directory throws this exception
            addProducerDirectoryAndInstallDir(details, user, uid, group, gid, mode);
        }
    }

    private void addProducerFile(FileCopyDetails fileDetails, File source, String user, int uid, String group, int gid, int mode) {
        dataProducers.add(new DataProducers.FileProducer(PackagingUtils.getRootPath(fileDetails), source, user, uid, group, gid, mode));
    }

    private void addProducerDirectoryAndInstallDir(FileCopyDetails dirDetails, String user, int uid, String group, int gid, int mode) {
        String rootPath = PackagingUtils.getRootPath(dirDetails);
        dataProducers.add(new DataProducers.DirProducer(rootPath, user, uid, group, gid, mode));
        // parent directories are created implicitly by jdeb
        installDirs.add(new DebCopyAction.InstallDir(rootPath, user, group));
    }

    private void addProducerLink(PackagingUtils.Symlink link) {
        if (links.add(link)) {
            dataProducers.add(new DataProducerLink(link.path(), link.target(), true, null, null, null));
        }
    }
}
