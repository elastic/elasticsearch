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
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.FileCopyDetails;
import org.redline_rpm.Builder;
import org.redline_rpm.header.Architecture;
import org.redline_rpm.header.Header;
import org.redline_rpm.header.Os;
import org.redline_rpm.header.RpmType;
import org.redline_rpm.payload.Directive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
        builder.setPackage(task.getPackageName(), task.getVersion(), release(), 0);
        builder.setType(RpmType.BINARY);
        Architecture architecture = task.getArchString() == null
            ? Architecture.NOARCH
            : Architecture.valueOf(task.getArchString().toUpperCase(Locale.ROOT));
        builder.setPlatform(architecture, task.getExten().getOs().getOrElse(Os.UNKNOWN));
        builder.setGroup(task.getPackageGroup());
        builder.setBuildHost(localHostName());
        builder.setSummary(task.getResolvedSummary().getOrElse(""));
        builder.setDescription(task.getResolvedPackageDescription().getOrElse(""));
        builder.setLicense(task.getLicense());
        builder.setPackager(task.getPackager());
        builder.setDistribution(task.getExten().getDistribution().getOrElse(""));
        builder.setVendor(task.getExten().getVendor().getOrElse(""));
        builder.setUrl(task.getResolvedUrl().getOrElse(""));
        List<String> prefixes = task.getExten().getPrefixes().getOrElse(List.of());
        if (prefixes.isEmpty() == false) {
            builder.setPrefixes(prefixes.toArray(new String[0]));
        }

        String signingKeyId = task.getResolvedSigningKeyId().getOrElse("");
        String signingKeyPassphrase = task.getResolvedSigningKeyPassphrase().getOrElse("");
        File signingKeyRingFile = task.getResolvedSigningKeyRingFile().isPresent()
            ? task.getResolvedSigningKeyRingFile().get().getAsFile()
            : null;
        if (signingKeyId.isBlank() == false
            && signingKeyPassphrase.isBlank() == false
            && signingKeyRingFile != null
            && signingKeyRingFile.exists()) {
            builder.setPrivateKeyId(signingKeyId);
            builder.setPrivateKeyPassphrase(signingKeyPassphrase);
            builder.setPrivateKeyRingFile(signingKeyRingFile);
        }

        // a source package is required, otherwise createrepo will assume the package is a source package
        String sourcePackage = task.getPackageName() + "-" + task.getVersion() + "-" + release() + "-src.rpm";
        builder.addHeaderEntry(Header.HeaderTag.SOURCERPM, sourcePackage);

        List<String> preInstall = task.getExten().getPreInstallCommands().getOrElse(List.of());
        if (preInstall.isEmpty() == false) {
            builder.setPreInstallScript(scriptWithDefines(preInstall));
        }
        List<String> postInstall = task.getExten().getPostInstallCommands().getOrElse(List.of());
        if (postInstall.isEmpty() == false) {
            builder.setPostInstallScript(scriptWithDefines(postInstall));
        }
        List<String> preUninstall = task.getExten().getPreUninstallCommands().getOrElse(List.of());
        if (preUninstall.isEmpty() == false) {
            builder.setPreUninstallScript(scriptWithDefines(preUninstall));
        }
        List<String> postUninstall = task.getExten().getPostUninstallCommands().getOrElse(List.of());
        if (postUninstall.isEmpty() == false) {
            builder.setPostUninstallScript(scriptWithDefines(postUninstall));
        }
        List<String> postTrans = task.getExten().getPostTransCommands().getOrElse(List.of());
        if (postTrans.isEmpty() == false) {
            builder.setPostTransScript(scriptWithDefines(postTrans));
        }

        rpmFileVisitorStrategy = new RpmFileVisitorStrategy(builder);
    }

    @Override
    protected void visitFile(FileCopyDetails fileDetails, CopySpec spec) {
        logger.debug("adding file {}", fileDetails.getRelativePath().getPathString());

        File inputFile = extractFile(fileDetails);

        Directive fileType = (Directive) SpecAttributes.lookup(spec, SpecAttributes.FILE_TYPE);
        String user = lookupOrDefault(spec, SpecAttributes.USER, taskUser());
        String group = lookupOrDefault(spec, SpecAttributes.PERMISSION_GROUP, taskGroup());

        Integer explicitMode = PackagingUtils.getFileMode(spec);
        int fileMode = explicitMode != null ? explicitMode : PackagingUtils.getUnixPermission(fileDetails);
        boolean addParentDirs = task.getExten().getAddParentDirs().getOrElse(true);

        rpmFileVisitorStrategy.addFile(fileDetails, inputFile, fileMode, -1, fileType, user, group, addParentDirs);
    }

    @Override
    protected void visitDir(FileCopyDetails dirDetails, CopySpec spec) {
        if (spec == null) {
            logger.info("got an empty spec for {}", dirDetails.getPath());
            return;
        }
        boolean createDirectoryEntry = lookupOrDefault(spec, SpecAttributes.CREATE_DIRECTORY_ENTRY, false);
        if (createDirectoryEntry == false) {
            return;
        }
        logger.debug("adding directory {}", dirDetails.getRelativePath().getPathString());

        Integer explicitDirMode = PackagingUtils.getDirMode(spec);
        int dirMode = explicitDirMode != null ? explicitDirMode : PackagingUtils.getUnixPermission(dirDetails);
        Directive directive = (Directive) SpecAttributes.lookup(spec, SpecAttributes.FILE_TYPE);
        String user = lookupOrDefault(spec, SpecAttributes.USER, taskUser());
        String group = lookupOrDefault(spec, SpecAttributes.PERMISSION_GROUP, taskGroup());
        boolean setgid = lookupOrDefault(spec, SpecAttributes.SETGID, false);
        if (setgid) {
            dirMode = dirMode | SETGID_BIT;
        }
        boolean addParentDirs = task.getExten().getAddParentDirs().getOrElse(true);
        rpmFileVisitorStrategy.addDirectory(dirDetails, dirMode, directive, user, group, addParentDirs);
    }

    private String taskUser() {
        return task.getResolvedUser().getOrNull();
    }

    private String taskGroup() {
        return task.getResolvedPermissionGroup().getOrNull();
    }

    private String release() {
        return task.getRelease() == null ? "" : task.getRelease();
    }

    @SuppressWarnings("unchecked")
    private static <T> T lookupOrDefault(CopySpec spec, String key, T taskDefault) {
        Object value = SpecAttributes.lookup(spec, key);
        return value != null ? (T) value : taskDefault;
    }

    @Override
    protected void addDependency(Dependency dependency) {
        builder.addDependency(dependency.packageName(), dependency.flag(), dependency.version());
    }

    @Override
    protected void addConflict(Dependency dependency) {
        builder.addConflicts(dependency.packageName(), dependency.flag(), dependency.version());
    }

    @Override
    protected void addObsolete(Dependency dependency) {
        builder.addObsoletes(dependency.packageName(), dependency.flag(), dependency.version());
    }

    @Override
    protected void addDirectory(Directory directory) {
        try {
            builder.addDirectory(directory.path(), directory.permissions(), null, taskUser(), taskGroup(), false);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        logger.info("created rpm {}", rpmFile);
    }

    private static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    /**
     * Prepends the standard {@code RPM_*} environment defines to a maintainer script, mirroring
     * the behavior of rpmbuild.
     */
    private String scriptWithDefines(List<String> scripts) {
        List<String> parts = new ArrayList<>();
        parts.add(
            String.format(
                Locale.ROOT,
                " RPM_ARCH=%s \n RPM_OS=%s \n RPM_PACKAGE_NAME=%s \n RPM_PACKAGE_VERSION=%s \n RPM_PACKAGE_RELEASE=%s \n\n",
                task.getArchString(),
                task.getOs() == null ? "" : task.getOs().toString().toLowerCase(Locale.ROOT),
                task.getPackageName(),
                task.getVersion(),
                release()
            )
        );
        parts.addAll(scripts);
        return concat(parts);
    }
}
