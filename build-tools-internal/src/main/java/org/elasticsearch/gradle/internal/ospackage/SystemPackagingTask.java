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
import org.gradle.api.Project;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.DuplicatesStrategy;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.AbstractCopyTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;
import org.gradle.work.DisableCachingByDefault;
import org.redline_rpm.header.Os;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.inject.Inject;

/**
 * Base class of the {@link org.elasticsearch.gradle.internal.ospackage.rpm.Rpm} and
 * {@link org.elasticsearch.gradle.internal.ospackage.deb.Deb} archive tasks. The package metadata
 * lives in the {@link SystemPackagingExtension} nested input block; project-wide defaults are
 * inherited from the {@code ospackage} extension both as property conventions and as a shared
 * copy spec appended to this task's root spec.
 */
@DisableCachingByDefault(because = "Packaging tasks are IO bound and not worth caching")
public abstract class SystemPackagingTask extends AbstractArchiveTask {

    private static final String HOST_NAME = localHostName();

    private final SystemPackagingExtension exten;
    private final ProjectPackagingExtension parentExten;

    public SystemPackagingTask() {
        super();
        exten = getObjectFactory().newInstance(SystemPackagingExtension.class);
        parentExten = getProject().getExtensions().findByType(ProjectPackagingExtension.class);
        if (parentExten != null) {
            getRootSpec().with(parentExten.getDelegateCopySpec());
        }
        setDuplicatesStrategy(DuplicatesStrategy.INCLUDE);
        getRootSpec().setDuplicatesStrategy(DuplicatesStrategy.INCLUDE);
        getMainSpec().setDuplicatesStrategy(DuplicatesStrategy.INCLUDE);
    }

    @Inject
    protected abstract ProviderFactory getProviderFactory();

    @Nested
    public SystemPackagingExtension getExten() {
        return exten;
    }

    @Internal
    protected ProjectPackagingExtension getParentExten() {
        return parentExten;
    }

    // ------------------------------------------------------------------
    // conventions
    // ------------------------------------------------------------------

    /**
     * Applies the default values and the project-level {@code ospackage} overrides as conventions,
     * so any value set directly on the task always wins. Called by the plugin when the task is
     * realized.
     */
    public void applyConventions(Project project) {
        exten.getPackageName().convention(parentOr(SystemPackagingExtension::getPackageName, getArchiveBaseName()));
        exten.getRelease().convention(parentOr(SystemPackagingExtension::getRelease, getArchiveClassifier()));
        exten.getEpoch().convention(parentOrValue(SystemPackagingExtension::getEpoch, 0));
        exten.getSigningKeyId().convention(parentOrValue(SystemPackagingExtension::getSigningKeyId, ""));
        exten.getSigningKeyPassphrase().convention(parentOrValue(SystemPackagingExtension::getSigningKeyPassphrase, ""));
        if (parentExten != null) {
            exten.getSigningKeyRingFile().convention(parentExten.getSigningKeyRingFile());
        } else {
            File defaultKeyRing = new File(System.getProperty("user.home"), ".gnupg/secring.gpg");
            if (defaultKeyRing.exists()) {
                exten.getSigningKeyRingFile().convention(getObjectFactory().fileProperty().fileValue(defaultKeyRing));
            }
        }
        exten.getPackager().convention(parentOrValue(SystemPackagingExtension::getPackager, System.getProperty("user.name", "")));
        exten.getUser().convention(parentOr(SystemPackagingExtension::getUser, exten.getPackager()));
        exten.getMaintainer().convention(parentOr(SystemPackagingExtension::getMaintainer, exten.getPackager()));
        exten.getUploaders().convention(parentOr(SystemPackagingExtension::getUploaders, exten.getPackager()));
        exten.getPermissionGroup().convention(parentOrValue(SystemPackagingExtension::getPermissionGroup, ""));
        exten.getSetgid().convention(parentOrValue(SystemPackagingExtension::getSetgid, false));
        exten.getBuildHost().convention(parentOrValue(SystemPackagingExtension::getBuildHost, HOST_NAME));
        exten.getSummary().convention(parentOr(SystemPackagingExtension::getSummary, exten.getPackageName()));
        String description = project.getDescription() == null ? "" : project.getDescription();
        exten.getPackageDescription().convention(parentOrValue(SystemPackagingExtension::getPackageDescription, description));
        exten.getLicense().convention(parentOrValue(SystemPackagingExtension::getLicense, ""));
        exten.getDistribution().convention(parentOrValue(SystemPackagingExtension::getDistribution, ""));
        exten.getVendor().convention(parentOrValue(SystemPackagingExtension::getVendor, ""));
        exten.getUrl().convention(parentOrValue(SystemPackagingExtension::getUrl, ""));
        exten.getSourcePackage().convention(parentOrValue(SystemPackagingExtension::getSourcePackage, ""));
        exten.getCreateDirectoryEntry().convention(parentOrValue(SystemPackagingExtension::getCreateDirectoryEntry, false));
        exten.getPriority().convention(parentOrValue(SystemPackagingExtension::getPriority, "optional"));
        if (parentExten != null) {
            exten.getPackageGroup().convention(parentExten.getPackageGroup());
            exten.getFileType().convention(parentExten.getFileType());
        }

        String sanitizedVersion = sanitizeVersion(project.getVersion().toString());
        exten.getVersion().convention(parentOrValue(SystemPackagingExtension::getVersion, sanitizedVersion));
        getArchiveVersion().convention(sanitizedVersion);
        getArchiveFileName().convention(getProviderFactory().provider(this::assembleArchiveName));
    }

    protected <T> Provider<T> parentOr(Function<SystemPackagingExtension, Property<T>> property, Provider<T> fallback) {
        return parentExten != null ? property.apply(parentExten).orElse(fallback) : fallback;
    }

    protected <T> Provider<T> parentOrValue(Function<SystemPackagingExtension, Property<T>> property, T fallback) {
        return parentExten != null ? property.apply(parentExten).orElse(fallback) : getProviderFactory().provider(() -> fallback);
    }

    private static String sanitizeVersion(String version) {
        if ("unspecified".equals(version)) {
            return "0";
        }
        return version.replaceAll("\\+.*", "").replace('-', '~');
    }

    private static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    /** Assembles the default archive file name, e.g. {@code name-version-release.arch.rpm}. */
    protected abstract String assembleArchiveName();

    // ------------------------------------------------------------------
    // task level DSL, delegating to the nested extension
    // ------------------------------------------------------------------

    @Internal
    public String getPackageName() {
        return exten.getPackageName().getOrNull();
    }

    public void setPackageName(String packageName) {
        exten.getPackageName().set(packageName);
    }

    @Internal
    public String getRelease() {
        return exten.getRelease().getOrNull();
    }

    public void setRelease(String release) {
        exten.getRelease().set(release);
    }

    /**
     * The package version; defaults to the sanitized project version. The archive version (file
     * name) is tracked separately, matching the original plugin behavior.
     */
    @Internal
    public String getVersion() {
        return exten.getVersion().getOrNull();
    }

    public void setVersion(String version) {
        exten.getVersion().set(version);
    }

    public void setArch(String arch) {
        exten.getArchStr().set(arch);
    }

    @Internal
    public String getArchString() {
        String archStr = exten.getArchStr().getOrNull();
        return archStr == null ? null : archStr.toLowerCase();
    }

    @Internal
    public String getUser() {
        return exten.getUser().getOrNull();
    }

    public void setUser(String user) {
        exten.getUser().set(user);
    }

    @Internal
    public String getPermissionGroup() {
        return exten.getPermissionGroup().getOrNull();
    }

    public void setPermissionGroup(String permissionGroup) {
        exten.getPermissionGroup().set(permissionGroup);
    }

    @Internal
    public String getPackageGroup() {
        return exten.getPackageGroup().getOrNull();
    }

    public void setPackageGroup(String packageGroup) {
        exten.getPackageGroup().set(packageGroup);
    }

    @Internal
    public String getMaintainer() {
        return exten.getMaintainer().getOrNull();
    }

    public void setMaintainer(String maintainer) {
        exten.getMaintainer().set(maintainer);
    }

    @Internal
    public String getLicense() {
        return exten.getLicense().getOrNull();
    }

    public void setLicense(String license) {
        exten.getLicense().set(license);
    }

    @Internal
    public String getPackager() {
        return exten.getPackager().getOrNull();
    }

    public void setPackager(String packager) {
        exten.getPackager().set(packager);
    }

    @Internal
    public String getDistribution() {
        return exten.getDistribution().getOrNull();
    }

    public void setDistribution(String distribution) {
        exten.getDistribution().set(distribution);
    }

    @Internal
    public String getVendor() {
        return exten.getVendor().getOrNull();
    }

    public void setVendor(String vendor) {
        exten.getVendor().set(vendor);
    }

    @Internal
    public Os getOs() {
        return exten.getOs().getOrNull();
    }

    public void setOs(Os os) {
        exten.getOs().set(os);
    }

    @Internal
    public Boolean getAddParentDirs() {
        return exten.getAddParentDirs().getOrNull();
    }

    public void setAddParentDirs(boolean addParentDirs) {
        exten.getAddParentDirs().set(addParentDirs);
    }

    @Internal
    public org.gradle.api.provider.MapProperty<String, String> getCustomFields() {
        return exten.getCustomFields();
    }

    public void prefix(String prefix) {
        exten.prefix(prefix);
    }

    public void configurationFile(String path) {
        exten.configurationFile(path);
    }

    public Directory directory(String path, int permissions) {
        return exten.directory(path, permissions);
    }

    public Dependency requires(String packageName) {
        return exten.requires(packageName);
    }

    public Dependency requires(String packageName, String version) {
        return exten.requires(packageName, version);
    }

    public Dependency requires(String packageName, String version, int flag) {
        return exten.requires(packageName, version, flag);
    }

    public Dependency obsoletes(String packageName, String version, int flag) {
        return exten.obsoletes(packageName, version, flag);
    }

    public Dependency conflicts(String packageName) {
        return exten.conflicts(packageName);
    }

    public void preInstall(File script) {
        exten.preInstall(script);
    }

    public void postInstall(File script) {
        exten.postInstall(script);
    }

    public void preUninstall(File script) {
        exten.preUninstall(script);
    }

    public void postUninstall(File script) {
        exten.postUninstall(script);
    }

    public void postTrans(File script) {
        exten.postTrans(script);
    }

    // ------------------------------------------------------------------
    // aggregation of task level and project level ("ospackage") values,
    // used by the copy actions at execution time
    // ------------------------------------------------------------------

    @Input
    @Optional
    public List<String> getAllConfigurationFiles() {
        return merged(SystemPackagingExtension::getConfigurationFiles);
    }

    @Input
    @Optional
    public List<String> getAllPreInstallCommands() {
        return merged(SystemPackagingExtension::getPreInstallCommands);
    }

    @Input
    @Optional
    public List<String> getAllPostInstallCommands() {
        return merged(SystemPackagingExtension::getPostInstallCommands);
    }

    @Input
    @Optional
    public List<String> getAllPreUninstallCommands() {
        return merged(SystemPackagingExtension::getPreUninstallCommands);
    }

    @Input
    @Optional
    public List<String> getAllPostUninstallCommands() {
        return merged(SystemPackagingExtension::getPostUninstallCommands);
    }

    @Input
    @Optional
    public List<String> getAllPostTransCommands() {
        return merged(SystemPackagingExtension::getPostTransCommands);
    }

    @Input
    @Optional
    public List<Dependency> getAllDependencies() {
        return merged(SystemPackagingExtension::getDependencies);
    }

    @Input
    @Optional
    public List<Dependency> getAllObsoletes() {
        return merged(SystemPackagingExtension::getObsoletes);
    }

    @Input
    @Optional
    public List<Dependency> getAllConflicts() {
        return merged(SystemPackagingExtension::getConflicts);
    }

    @Input
    @Optional
    public List<String> getAllPrefixes() {
        return merged(SystemPackagingExtension::getPrefixes).stream().distinct().toList();
    }

    @Input
    @Optional
    public Map<String, String> getAllCustomFields() {
        Map<String, String> result = new LinkedHashMap<>(exten.getCustomFields().getOrElse(Map.of()));
        if (parentExten != null) {
            parentExten.getCustomFields().getOrElse(Map.of()).forEach(result::putIfAbsent);
        }
        return result;
    }

    @Input
    @Optional
    public List<Directory> getAllDirectories() {
        List<Directory> result = new ArrayList<>(exten.getDirectories());
        if (parentExten != null) {
            result.addAll(parentExten.getDirectories());
        }
        return result;
    }

    private <T> List<T> merged(Function<SystemPackagingExtension, org.gradle.api.provider.ListProperty<T>> property) {
        List<T> result = new ArrayList<>(property.apply(exten).getOrElse(List.of()));
        if (parentExten != null) {
            result.addAll(property.apply(parentExten).getOrElse(List.of()));
        }
        return result;
    }

    // ------------------------------------------------------------------
    // copy spec configuration with packaging attributes
    // ------------------------------------------------------------------

    @Override
    public AbstractCopyTask from(Object sourcePath, Closure closure) {
        getMainSpec().from(sourcePath, (Action<? super CopySpec>) spec -> EnhancedCopySpec.configure(closure, spec, this));
        return this;
    }

    @Override
    public AbstractArchiveTask into(Object destPath, Closure closure) {
        getMainSpec().into(destPath, (Action<? super CopySpec>) spec -> EnhancedCopySpec.configure(closure, spec, this));
        return this;
    }

    /**
     * Declares an input file collection annotated with {@code @SkipWhenEmpty} as a workaround to
     * force building the package even if no {@code from} clause is declared; without it the task
     * would be marked NO-SOURCE. The provided file collection is not used anywhere else.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    @SkipWhenEmpty
    protected FileCollection getFakeFiles() {
        return getObjectFactory().fileCollection().from("fake");
    }
}
