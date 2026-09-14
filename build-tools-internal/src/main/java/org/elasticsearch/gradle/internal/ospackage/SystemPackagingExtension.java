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

package org.elasticsearch.gradle.internal.ospackage;

import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.redline_rpm.header.Os;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

/**
 * The metadata shared by rpm and deb packaging: package identity, ownership defaults, package
 * relationships, maintainer scripts and explicitly owned directories. Used both as the
 * project-level {@code ospackage} extension (see {@link ProjectPackagingExtension}) and as the
 * per-task nested input block of {@link SystemPackagingTask}.
 */
public abstract class SystemPackagingExtension {

    /**
     * Deliberately a plain mutable list rather than a {@code ListProperty}: the Elasticsearch
     * distribution build registers additional directory entries from {@code eachFile} callbacks
     * while the task is executing, after Gradle has finalized all {@code Property} values. A
     * {@code ListProperty} would fail with "property is final and cannot be changed".
     */
    private final List<Directory> directories = new ArrayList<>();

    @Inject
    protected abstract ObjectFactory getObjects();

    @Inject
    protected abstract ProviderFactory getProviders();

    @Input
    @Optional
    public abstract Property<String> getPackageName();

    @Input
    @Optional
    public abstract Property<String> getRelease();

    /**
     * The package version. Unlike the archive version (which controls the file name and can
     * contain arbitrary characters) this must follow the version rules of the target package
     * manager; it defaults to the project version with {@code -} replaced by {@code ~}.
     */
    @Input
    @Optional
    public abstract Property<String> getVersion();

    /** Default file owner within the package. */
    @Input
    @Optional
    public abstract Property<String> getUser();

    /** Default file group within the package. */
    @Input
    @Optional
    public abstract Property<String> getPermissionGroup();

    /** The rpm "Group" respectively the debian "Section" of the package. */
    @Input
    @Optional
    public abstract Property<String> getPackageGroup();

    @Input
    @Optional
    public abstract Property<String> getSummary();

    @Input
    @Optional
    public abstract Property<String> getPackageDescription();

    /** RPM only. */
    @Input
    @Optional
    public abstract Property<String> getLicense();

    /** RPM only. */
    @Input
    @Optional
    public abstract Property<String> getPackager();

    /** RPM only. */
    @Input
    @Optional
    public abstract Property<String> getDistribution();

    /** RPM only. */
    @Input
    @Optional
    public abstract Property<String> getVendor();

    @Input
    @Optional
    public abstract Property<String> getUrl();

    /** The package architecture as understood by the target package manager. */
    @Input
    @Optional
    public abstract Property<String> getArchStr();

    /** DEB only. */
    @Input
    @Optional
    public abstract Property<String> getMaintainer();

    @Input
    @Optional
    public abstract Property<String> getSigningKeyId();

    @Input
    @Optional
    public abstract Property<String> getSigningKeyPassphrase();

    @InputFile
    @Optional
    @PathSensitive(PathSensitivity.ABSOLUTE)
    public abstract RegularFileProperty getSigningKeyRingFile();

    /** DEB only: script file installed verbatim as the {@code preinst} maintainer script. */
    @InputFile
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getPreInstallFile();

    /** DEB only: script file installed verbatim as the {@code postinst} maintainer script. */
    @InputFile
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getPostInstallFile();

    /** DEB only: script file installed verbatim as the {@code prerm} maintainer script. */
    @InputFile
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getPreUninstallFile();

    /** DEB only: script file installed verbatim as the {@code postrm} maintainer script. */
    @InputFile
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getPostUninstallFile();

    /** RPM only. */
    @Input
    @Optional
    public abstract Property<Os> getOs();

    /** RPM only: whether parent directories of packaged files are implicitly owned. */
    @Input
    @Optional
    public abstract Property<Boolean> getAddParentDirs();

    /** RPM only: relocation prefixes. */
    @Input
    @Optional
    public abstract ListProperty<String> getPrefixes();

    /** Paths marked as configuration files in the package metadata. */
    @Input
    @Optional
    public abstract ListProperty<String> getConfigurationFiles();

    @Input
    @Optional
    public abstract ListProperty<String> getPreInstallCommands();

    @Input
    @Optional
    public abstract ListProperty<String> getPostInstallCommands();

    @Input
    @Optional
    public abstract ListProperty<String> getPreUninstallCommands();

    @Input
    @Optional
    public abstract ListProperty<String> getPostUninstallCommands();

    /** RPM only: %posttrans scriptlet. */
    @Input
    @Optional
    public abstract ListProperty<String> getPostTransCommands();

    @Input
    @Optional
    public abstract ListProperty<Dependency> getDependencies();

    /** RPM only. */
    @Input
    @Optional
    public abstract ListProperty<Dependency> getObsoletes();

    @Input
    @Optional
    public abstract ListProperty<Dependency> getConflicts();

    /** DEB only: custom control file fields, rendered as {@code XB-<Key>}. */
    @Input
    @Optional
    public abstract MapProperty<String, String> getCustomFields();

    @Input
    @Optional
    public List<Directory> getDirectories() {
        return directories;
    }

    // ------------------------------------------------------------------
    // convenience DSL methods used by the Elasticsearch packaging build
    // ------------------------------------------------------------------

    public Dependency requires(String packageName, String version, int flag) {
        Dependency dep = new Dependency(packageName, version, flag);
        getDependencies().add(dep);
        return dep;
    }

    public Dependency requires(String packageName) {
        return requires(packageName, "", 0);
    }

    public Dependency obsoletes(String packageName, String version, int flag) {
        Dependency dep = new Dependency(packageName, version, flag);
        getObsoletes().add(dep);
        return dep;
    }

    public Dependency conflicts(String packageName) {
        Dependency dep = new Dependency(packageName, "", 0);
        getConflicts().add(dep);
        return dep;
    }

    public void prefix(String prefix) {
        getPrefixes().add(prefix);
    }

    public void configurationFile(String path) {
        getConfigurationFiles().add(path);
    }

    public Directory directory(String path, int permissions) {
        Directory directory = new Directory(path, permissions);
        directories.add(directory);
        return directory;
    }

    /**
     * The maintainer script methods record the script twice: as a file (used verbatim by the deb
     * packaging) and as script <em>content</em> read lazily via a
     * {@link ProviderFactory#fileContents} provider (used by the rpm packaging with the standard
     * defines prepended), so the file is read at execution time and participates correctly in
     * configuration cache invalidation.
     */
    public void preInstall(File script) {
        getPreInstallFile().fileValue(script);
        getPreInstallCommands().addAll(contentsOf(script));
    }

    public void postInstall(File script) {
        getPostInstallFile().fileValue(script);
        getPostInstallCommands().addAll(contentsOf(script));
    }

    public void preUninstall(File script) {
        getPreUninstallFile().fileValue(script);
        getPreUninstallCommands().addAll(contentsOf(script));
    }

    public void postUninstall(File script) {
        getPostUninstallFile().fileValue(script);
        getPostUninstallCommands().addAll(contentsOf(script));
    }

    public void postTrans(File script) {
        getPostTransCommands().addAll(contentsOf(script));
    }

    private Provider<List<String>> contentsOf(File script) {
        RegularFileProperty fileProperty = getObjects().fileProperty();
        fileProperty.set(script);
        return getProviders().fileContents(fileProperty)
            .getAsText()
            .orElse("")
            .map(content -> content.isEmpty() ? List.of() : List.of(content));
    }
}
