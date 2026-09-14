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

import org.elasticsearch.gradle.internal.ospackage.AbstractPackagingCopyAction;
import org.elasticsearch.gradle.internal.ospackage.Dependency;
import org.elasticsearch.gradle.internal.ospackage.Directory;
import org.elasticsearch.gradle.internal.ospackage.PackagingUtils;
import org.elasticsearch.gradle.internal.ospackage.SpecAttributes;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.internal.file.copy.CopySpecInternal;
import org.gradle.api.internal.file.copy.FileCopyDetailsInternal;
import org.redline_rpm.Builder;
import org.redline_rpm.header.Architecture;
import org.redline_rpm.header.Header;
import org.redline_rpm.payload.Directive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds the rpm file for a {@link Rpm} task using the redline library.
 */
class RpmCopyAction extends AbstractPackagingCopyAction<Rpm> {

    private static final Logger logger = LoggerFactory.getLogger(RpmCopyAction.class);
    private static final int SETGID_BIT = 02000;

    private Builder builder;
    private RpmFileVisitorStrategy rpmFileVisitorStrategy;

    RpmCopyAction(Rpm task) {
        super(task);
        validate(task);
    }

    private static void validate(Rpm task) {
        String packageName = task.getPackageName();
        if (packageName == null || packageName.matches("[a-zA-Z0-9-._+]+") == false) {
            throw new InvalidUserDataException(
                "Invalid package name '" + packageName + "' - a valid package name must only contain [a-zA-Z0-9-._+]"
            );
        }
        if (task.getVersion() == null) {
            throw new InvalidUserDataException("RPM requires a version string");
        }
    }

    @Override
    protected void startVisit() {
        super.startVisit();

        builder = new Builder();
        builder.setPackage(task.getPackageName(), task.getVersion(), task.getRelease(), task.getExten().getEpoch().get());
        builder.setType(task.getExten().getType().get());
        builder.setPlatform(Architecture.valueOf(task.getArchString().toUpperCase()), task.getExten().getOs().get());
        builder.setGroup(task.getPackageGroup());
        builder.setBuildHost(task.getExten().getBuildHost().get());
        builder.setSummary(task.getExten().getSummary().get());
        builder.setDescription(task.getExten().getPackageDescription().getOrElse(""));
        builder.setLicense(task.getLicense());
        builder.setPackager(task.getPackager());
        builder.setDistribution(task.getDistribution());
        builder.setVendor(task.getVendor());
        builder.setUrl(task.getExten().getUrl().get());
        List<String> prefixes = task.getAllPrefixes();
        if (prefixes.isEmpty() == false) {
            builder.setPrefixes(prefixes.toArray(new String[0]));
        }

        String signingKeyId = task.getExten().getSigningKeyId().getOrElse("");
        String signingKeyPassphrase = task.getExten().getSigningKeyPassphrase().getOrElse("");
        File signingKeyRingFile = task.getExten().getSigningKeyRingFile().isPresent()
            ? task.getExten().getSigningKeyRingFile().get().getAsFile()
            : null;
        if (signingKeyId.isBlank() == false
            && signingKeyPassphrase.isBlank() == false
            && signingKeyRingFile != null
            && signingKeyRingFile.exists()) {
            builder.setPrivateKeyId(signingKeyId);
            builder.setPrivateKeyPassphrase(signingKeyPassphrase);
            builder.setPrivateKeyRingFile(signingKeyRingFile);
        }

        String sourcePackage = task.getExten().getSourcePackage().getOrElse("");
        if (sourcePackage.isEmpty()) {
            // a source package is required, otherwise createrepo will assume the package is a source package
            sourcePackage = task.getPackageName() + "-" + task.getVersion() + "-" + task.getRelease() + "-src.rpm";
        }
        builder.addHeaderEntry(Header.HeaderTag.SOURCERPM, sourcePackage);

        if (task.getAllPreInstallCommands().isEmpty() == false) {
            builder.setPreInstallScript(scriptWithDefines(task.getAllPreInstallCommands()));
        }
        if (task.getAllPostInstallCommands().isEmpty() == false) {
            builder.setPostInstallScript(scriptWithDefines(task.getAllPostInstallCommands()));
        }
        if (task.getAllPreUninstallCommands().isEmpty() == false) {
            builder.setPreUninstallScript(scriptWithDefines(task.getAllPreUninstallCommands()));
        }
        if (task.getAllPostUninstallCommands().isEmpty() == false) {
            builder.setPostUninstallScript(scriptWithDefines(task.getAllPostUninstallCommands()));
        }
        if (task.getAllPostTransCommands().isEmpty() == false) {
            builder.setPostTransScript(scriptWithDefines(task.getAllPostTransCommands()));
        }

        rpmFileVisitorStrategy = new RpmFileVisitorStrategy(builder);
    }

    @Override
    protected void visitFile(FileCopyDetailsInternal fileDetails, CopySpecInternal spec) {
        logger.debug("adding file {}", fileDetails.getRelativePath().getPathString());

        File inputFile = extractFile(fileDetails);

        Directive fileType = (Directive) SpecAttributes.lookup(spec, SpecAttributes.FILE_TYPE);
        String user = lookupOrDefault(spec, SpecAttributes.USER, task.getUser());
        String group = lookupOrDefault(spec, SpecAttributes.PERMISSION_GROUP, task.getPermissionGroup());

        Integer explicitMode = PackagingUtils.getFileMode(spec);
        int fileMode = explicitMode != null ? explicitMode : PackagingUtils.getUnixPermission(fileDetails);
        boolean addParentDirs = lookupOrDefault(spec, SpecAttributes.ADD_PARENT_DIRS, task.getAddParentDirs());

        rpmFileVisitorStrategy.addFile(fileDetails, inputFile, fileMode, -1, fileType, user, group, addParentDirs);
    }

    @Override
    protected void visitDir(FileCopyDetailsInternal dirDetails, CopySpecInternal spec) {
        if (spec == null) {
            logger.info("got an empty spec for {}", dirDetails.getPath());
            return;
        }
        boolean createDirectoryEntry = lookupOrDefault(
            spec,
            SpecAttributes.CREATE_DIRECTORY_ENTRY,
            task.getExten().getCreateDirectoryEntry().get()
        );
        boolean addParentDirs = lookupOrDefault(spec, SpecAttributes.ADD_PARENT_DIRS, task.getAddParentDirs());
        if (createDirectoryEntry == false) {
            return;
        }
        logger.debug("adding directory {}", dirDetails.getRelativePath().getPathString());

        Integer explicitDirMode = PackagingUtils.getDirMode(spec);
        int dirMode = explicitDirMode != null ? explicitDirMode : PackagingUtils.getUnixPermission(dirDetails);
        Directive directive = (Directive) SpecAttributes.lookup(spec, SpecAttributes.FILE_TYPE);
        if (directive == null) {
            directive = task.getExten().getFileType().getOrNull();
        }
        String user = lookupOrDefault(spec, SpecAttributes.USER, task.getUser());
        String group = lookupOrDefault(spec, SpecAttributes.PERMISSION_GROUP, task.getPermissionGroup());
        boolean setgid = lookupOrDefault(spec, SpecAttributes.SETGID, task.getExten().getSetgid().get());
        if (setgid) {
            dirMode = dirMode | SETGID_BIT;
        }
        rpmFileVisitorStrategy.addDirectory(dirDetails, dirMode, directive, user, group, addParentDirs);
    }

    @SuppressWarnings("unchecked")
    private static <T> T lookupOrDefault(CopySpecInternal spec, String key, T taskDefault) {
        Object value = SpecAttributes.lookup(spec, key);
        return value != null ? (T) value : taskDefault;
    }

    @Override
    protected void addDependency(Dependency dependency) {
        builder.addDependency(dependency.getPackageName(), dependency.getFlag(), dependency.getVersion());
    }

    @Override
    protected void addConflict(Dependency dependency) {
        builder.addConflicts(dependency.getPackageName(), dependency.getFlag(), dependency.getVersion());
    }

    @Override
    protected void addObsolete(Dependency dependency) {
        builder.addObsoletes(dependency.getPackageName(), dependency.getFlag(), dependency.getVersion());
    }

    @Override
    protected void addDirectory(Directory directory) {
        try {
            builder.addDirectory(
                directory.getPath(),
                directory.getPermissions(),
                null,
                task.getUser(),
                task.getPermissionGroup(),
                directory.isAddParents()
            );
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @Override
    protected void end() throws IOException {
        File rpmFile = task.getArchiveFile().get().getAsFile();
        if (rpmFile.exists() && rpmFile.delete() == false) {
            throw new IOException("Cannot delete existing rpm file " + rpmFile);
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(rpmFile, "rw")) {
            builder.build(randomAccessFile.getChannel());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        logger.info("created rpm {}", rpmFile);
    }

    /**
     * Prepends the standard {@code RPM_*} environment defines to a maintainer script, mirroring
     * the behavior of rpmbuild.
     */
    private String scriptWithDefines(List<String> scripts) {
        List<String> parts = new ArrayList<>();
        parts.add(
            String.format(
                java.util.Locale.ROOT,
                " RPM_ARCH=%s \n RPM_OS=%s \n RPM_PACKAGE_NAME=%s \n RPM_PACKAGE_VERSION=%s \n RPM_PACKAGE_RELEASE=%s \n\n",
                task.getArchString(),
                task.getOs() == null ? "" : task.getOs().toString().toLowerCase(java.util.Locale.ROOT),
                task.getPackageName(),
                task.getVersion(),
                task.getRelease()
            )
        );
        parts.addAll(scripts);
        return concat(parts);
    }
}
