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

import groovy.lang.Closure;

import org.gradle.api.Action;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.DuplicatesStrategy;
import org.gradle.api.file.RegularFile;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.AbstractCopyTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;
import org.gradle.work.DisableCachingByDefault;
import org.redline_rpm.header.Os;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * Base class of the {@link org.elasticsearch.gradle.internal.ospackage.rpm.Rpm} and
 * {@link org.elasticsearch.gradle.internal.ospackage.deb.Deb} archive tasks. The package metadata
 * lives in the {@link SystemPackagingExtension} nested input block. Project-wide defaults from the
 * {@code ospackage} extension are not copied onto the task; the extension is tracked as a nested
 * input and values set on the task take precedence over the extension values via the
 * {@code resolve*} accessors when the package is built.
 */
@DisableCachingByDefault(because = "Packaging tasks are IO bound and not worth caching")
public abstract class SystemPackagingTask extends AbstractArchiveTask {

    private ProjectPackagingExtension parentExtension;

    public SystemPackagingTask() {
        super();
        setDuplicatesStrategy(DuplicatesStrategy.INCLUDE);
    }

    /** The package metadata of this task; automatically instantiated by Gradle. */
    @Nested
    public abstract SystemPackagingExtension getExten();

    /** The version used when none is set explicitly; wired by {@link OsPackageBasePlugin}. */
    @Input
    @Optional
    public abstract Property<String> getDefaultVersion();

    /**
     * Wires the project-level packaging defaults into this task: the shared {@code ospackage} copy
     * spec is appended to this task's specs, the extension is kept for value resolution and the
     * sanitized project version becomes the default package version. Called by the plugin when the
     * task is realized.
     */
    public void initDefaults(ProjectPackagingExtension parentExtension, String projectVersion) {
        this.parentExtension = parentExtension;
        getDefaultVersion().set(sanitizeVersion(projectVersion));
        with(parentExtension.getDelegateCopySpec());
    }

    private static String sanitizeVersion(String version) {
        if ("unspecified".equals(version)) {
            return "0";
        }
        return version.replaceAll("\\+.*", "").replace('-', '~');
    }

    // ------------------------------------------------------------------
    // task level DSL, delegating to the nested extension
    // ------------------------------------------------------------------

    @Internal
    public String getPackageName() {
        return getExten().getPackageName().getOrNull();
    }

    public void setPackageName(String packageName) {
        getExten().getPackageName().set(packageName);
    }

    @Internal
    public String getRelease() {
        return getExten().getRelease().getOrNull();
    }

    public void setRelease(String release) {
        getExten().getRelease().set(release);
    }

    /**
     * The package version; defaults to the sanitized project version. The archive version (file
     * name) is tracked separately, matching the original plugin behavior.
     */
    @Internal
    public String getVersion() {
        return getExten().getVersion().orElse(getDefaultVersion()).getOrNull();
    }

    public void setVersion(String version) {
        getExten().getVersion().set(version);
    }

    public void setArch(String arch) {
        getExten().getArchStr().set(arch);
    }

    @Internal
    public String getArchString() {
        String archStr = getExten().getArchStr().getOrNull();
        return archStr == null ? null : archStr.toLowerCase(Locale.ROOT);
    }

    @Internal
    public String getPackageGroup() {
        return getExten().getPackageGroup().getOrNull();
    }

    public void setPackageGroup(String packageGroup) {
        getExten().getPackageGroup().set(packageGroup);
    }

    @Internal
    public String getLicense() {
        return getExten().getLicense().getOrNull();
    }

    public void setLicense(String license) {
        getExten().getLicense().set(license);
    }

    @Internal
    public String getPackager() {
        return getExten().getPackager().getOrNull();
    }

    public void setPackager(String packager) {
        getExten().getPackager().set(packager);
    }

    @Internal
    public String getDistribution() {
        return getExten().getDistribution().getOrNull();
    }

    public void setDistribution(String distribution) {
        getExten().getDistribution().set(distribution);
    }

    @Internal
    public String getVendor() {
        return getExten().getVendor().getOrNull();
    }

    public void setVendor(String vendor) {
        getExten().getVendor().set(vendor);
    }

    @Internal
    public Os getOs() {
        return getExten().getOs().getOrNull();
    }

    public void setOs(Os os) {
        getExten().getOs().set(os);
    }

    @Internal
    public Boolean getAddParentDirs() {
        return getExten().getAddParentDirs().getOrNull();
    }

    public void setAddParentDirs(boolean addParentDirs) {
        getExten().getAddParentDirs().set(addParentDirs);
    }

    @Internal
    public org.gradle.api.provider.MapProperty<String, String> getCustomFields() {
        return getExten().getCustomFields();
    }

    public void prefix(String prefix) {
        getExten().prefix(prefix);
    }

    public void configurationFile(String path) {
        getExten().configurationFile(path);
    }

    public Directory directory(String path, int permissions) {
        return getExten().directory(path, permissions);
    }

    public Dependency requires(String packageName) {
        return getExten().requires(packageName);
    }

    public Dependency requires(String packageName, String version, int flag) {
        return getExten().requires(packageName, version, flag);
    }

    public Dependency obsoletes(String packageName, String version, int flag) {
        return getExten().obsoletes(packageName, version, flag);
    }

    public Dependency conflicts(String packageName) {
        return getExten().conflicts(packageName);
    }

    public void preInstall(File script) {
        getExten().preInstall(script);
    }

    public void postInstall(File script) {
        getExten().postInstall(script);
    }

    public void preUninstall(File script) {
        getExten().preUninstall(script);
    }

    public void postUninstall(File script) {
        getExten().postUninstall(script);
    }

    public void postTrans(File script) {
        getExten().postTrans(script);
    }

    // ------------------------------------------------------------------
    // value resolution: task values win over the ospackage extension.
    // The resolved providers are the task inputs; the extension itself is
    // deliberately not tracked as a nested input because it is shared by
    // all packaging tasks of the project (a shared decorated bean gets an
    // owner reference to one task, which the other tasks then could not
    // serialize into the configuration cache).
    // ------------------------------------------------------------------

    @Input
    @Optional
    public Provider<String> getResolvedUser() {
        return resolve(SystemPackagingExtension::getUser);
    }

    @Input
    @Optional
    public Provider<String> getResolvedPermissionGroup() {
        return resolve(SystemPackagingExtension::getPermissionGroup);
    }

    @Input
    @Optional
    public Provider<String> getResolvedMaintainer() {
        return resolve(SystemPackagingExtension::getMaintainer);
    }

    @Input
    @Optional
    public Provider<String> getResolvedSummary() {
        return resolve(SystemPackagingExtension::getSummary);
    }

    @Input
    @Optional
    public Provider<String> getResolvedPackageDescription() {
        return resolve(SystemPackagingExtension::getPackageDescription);
    }

    @Input
    @Optional
    public Provider<String> getResolvedUrl() {
        return resolve(SystemPackagingExtension::getUrl);
    }

    @Input
    @Optional
    public Provider<String> getResolvedSigningKeyId() {
        return resolve(SystemPackagingExtension::getSigningKeyId);
    }

    @Internal("tracked via the signing key ring file input")
    public Provider<String> getResolvedSigningKeyPassphrase() {
        return resolve(SystemPackagingExtension::getSigningKeyPassphrase);
    }

    @InputFile
    @Optional
    @PathSensitive(PathSensitivity.ABSOLUTE)
    public Provider<RegularFile> getResolvedSigningKeyRingFile() {
        return resolve(SystemPackagingExtension::getSigningKeyRingFile);
    }

    /** Requirements declared on the task combined with the ones from the {@code ospackage} extension. */
    @Input
    @Optional
    public Provider<List<Dependency>> getResolvedDependencies() {
        if (parentExtension == null) {
            return getExten().getDependencies();
        }
        return getExten().getDependencies().zip(parentExtension.getDependencies(), (own, shared) -> {
            List<Dependency> merged = new ArrayList<>(own);
            merged.addAll(shared);
            return merged;
        });
    }

    private <T> Provider<T> resolve(Function<SystemPackagingExtension, ? extends Provider<T>> property) {
        Provider<T> own = property.apply(getExten());
        return parentExtension != null ? own.orElse(property.apply(parentExtension)) : own;
    }

    // ------------------------------------------------------------------
    // copy spec configuration with packaging attributes
    // ------------------------------------------------------------------

    @Override
    public AbstractCopyTask from(Object sourcePath, Closure closure) {
        return from(sourcePath, (Action<? super CopySpec>) spec -> EnhancedCopySpec.configure(closure, spec, this));
    }

    @Override
    public AbstractArchiveTask into(Object destPath, Closure closure) {
        into(destPath, (Action<? super CopySpec>) spec -> EnhancedCopySpec.configure(closure, spec, this));
        return this;
    }
}
