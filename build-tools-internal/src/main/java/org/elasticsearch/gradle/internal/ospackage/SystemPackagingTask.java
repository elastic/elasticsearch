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

import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileTree;
import org.gradle.api.file.FileTreeElement;
import org.gradle.api.file.FileVisitDetails;
import org.gradle.api.file.FileVisitor;
import org.gradle.api.file.RegularFile;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternSet;
import org.gradle.work.DisableCachingByDefault;
import org.redline_rpm.header.Os;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import javax.inject.Inject;

/**
 * Base class of the {@link org.elasticsearch.gradle.internal.ospackage.rpm.Rpm} and
 * {@link org.elasticsearch.gradle.internal.ospackage.deb.Deb} tasks. Package content is declared
 * as {@link PackageContent} mappings of source trees onto destination paths with explicit
 * packaging metadata; the task walks the sources directly (preserving in-tree symbolic links) and
 * streams the entries to a package-format specific {@link PackageWriter}. Only public Gradle API
 * is used: the task is not a copy task and performs no intermediate staging.
 */
@DisableCachingByDefault(because = "Packaging tasks are IO bound and not worth caching")
public abstract class SystemPackagingTask extends DefaultTask {

    private final List<PackageContent> contents = new ArrayList<>();
    private final Provider<RegularFile> archiveFile;

    public SystemPackagingTask() {
        archiveFile = getDestinationDirectory().file(getArchiveFileName());
    }

    @Inject
    protected abstract ObjectFactory getObjectFactory();

    @Inject
    protected abstract ProviderFactory getProviderFactory();

    // ------------------------------------------------------------------
    // output
    // ------------------------------------------------------------------

    @Internal
    public abstract org.gradle.api.file.DirectoryProperty getDestinationDirectory();

    @Internal
    public abstract Property<String> getArchiveFileName();

    @OutputFile
    public Provider<RegularFile> getArchiveFile() {
        return archiveFile;
    }

    // ------------------------------------------------------------------
    // package metadata
    // ------------------------------------------------------------------

    @Input
    public abstract Property<String> getPackageName();

    /** The package version, following the version rules of the target package manager. */
    @Input
    public abstract Property<String> getVersion();

    @Input
    @Optional
    public abstract Property<String> getRelease();

    /** The package architecture as understood by the target package manager. */
    @Input
    @Optional
    public abstract Property<String> getArch();

    /** Default owner for packaged entries without explicit mapping ownership. */
    @Input
    @Optional
    public abstract Property<String> getUser();

    /** Default group for packaged entries without explicit mapping ownership. */
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

    /** DEB only. */
    @Input
    @Optional
    public abstract Property<String> getMaintainer();

    /** RPM only. */
    @Input
    @Optional
    public abstract Property<Os> getOs();

    @Input
    @Optional
    public abstract Property<String> getSigningKeyId();

    @Internal("secret; the key ring file is the tracked input")
    public abstract Property<String> getSigningKeyPassphrase();

    @InputFile
    @Optional
    @PathSensitive(PathSensitivity.ABSOLUTE)
    public abstract RegularFileProperty getSigningKeyRingFile();

    /** RPM only: relocation prefixes. */
    @Input
    @Optional
    public abstract ListProperty<String> getPrefixes();

    /** DEB only: paths listed in the conffiles control file. */
    @Input
    @Optional
    public abstract ListProperty<String> getConfigurationFiles();

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

    /** Explicit package-owned directory entries, e.g. otherwise empty state directories. */
    @Input
    @Optional
    public abstract ListProperty<Directory> getDirectories();

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

    /** The content mappings of this package. */
    @Nested
    public List<PackageContent> getContents() {
        return contents;
    }

    // ------------------------------------------------------------------
    // DSL
    // ------------------------------------------------------------------

    /** Maps source files or directories onto a destination inside the package. */
    public PackageContent from(Object source, Action<? super PackageContent> action) {
        PackageContent content = getObjectFactory().newInstance(PackageContent.class);
        content.getSource().from(source);
        action.execute(content);
        contents.add(content);
        return content;
    }

    /** The lower-cased architecture string used in package file names. */
    @Internal
    public String getArchString() {
        String arch = getArch().getOrNull();
        return arch == null ? null : arch.toLowerCase(Locale.ROOT);
    }

    public Dependency requires(String packageName) {
        return requires(packageName, "", 0);
    }

    public Dependency requires(String packageName, String version, int flag) {
        Dependency dep = new Dependency(packageName, version, flag);
        getDependencies().add(dep);
        return dep;
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
        return directory(path, permissions, null, null, false);
    }

    public Directory directory(String path, int permissions, String user, String permissionGroup, boolean setgid) {
        Directory directory = new Directory(path, permissions, user, permissionGroup, setgid);
        getDirectories().add(directory);
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
        RegularFileProperty fileProperty = getObjectFactory().fileProperty();
        fileProperty.set(script);
        return getProviderFactory().fileContents(fileProperty)
            .getAsText()
            .orElse("")
            .map(content -> content.isEmpty() ? List.of() : List.of(content));
    }

    // ------------------------------------------------------------------
    // package assembly
    // ------------------------------------------------------------------

    /** Creates the package-format specific writer for this task. */
    protected abstract PackageWriter createWriter() throws IOException;

    @TaskAction
    public void buildPackage() throws IOException {
        File archive = getArchiveFile().get().getAsFile();
        File parent = archive.getParentFile();
        if (parent != null && parent.mkdirs() == false && parent.isDirectory() == false) {
            throw new IOException("Cannot create destination directory " + parent);
        }
        PackageWriter writer = createWriter();
        Set<String> parentDirectories = new LinkedHashSet<>();
        // explicit directory entries first: deb creates missing parent directories implicitly on
        // first use, which would otherwise win over an explicitly declared entry for the same path
        for (Directory directory : getDirectories().get()) {
            int mode = directory.setgid() ? directory.permissions() | 02000 : directory.permissions();
            String user = directory.user() != null ? directory.user() : getUser().getOrNull();
            String group = directory.permissionGroup() != null ? directory.permissionGroup() : getPermissionGroup().getOrNull();
            writer.addDirectory(directory.path(), mode, user, group);
        }
        for (PackageContent content : contents) {
            visitContent(content, writer, parentDirectories);
        }
        for (String dir : parentDirectories) {
            writer.addDirectory(dir, 0755, getUser().getOrNull(), getPermissionGroup().getOrNull());
        }
        writer.finish();
    }

    private void visitContent(PackageContent content, PackageWriter writer, Set<String> parentDirectories) throws IOException {
        String into = requireAbsolute(content.getInto().get());
        String user = content.getUser().getOrElse(getUser().getOrNull());
        String group = content.getPermissionGroup().getOrElse(getPermissionGroup().getOrNull());
        int fileTypeFlags = content.getFileType().getOrElse(0);
        List<PermissionRuleMatcher> rules = compileRules(content);

        for (File root : content.getSource().getFiles()) {
            if (root.isDirectory()) {
                visitTree(content, root, into, user, group, fileTypeFlags, rules, writer, parentDirectories);
            } else if (root.isFile()) {
                String name = content.getRename().getOrElse(root.getName());
                String path = into + "/" + name;
                int mode = PackagingUtils.getUnixPermission(content.getFileMode().getOrElse(0644), root);
                writer.addFile(path, root, mode, user, group, fileTypeFlags);
                registerParentDirectories(content, path, parentDirectories);
            }
        }
    }

    private void visitTree(
        PackageContent content,
        File root,
        String into,
        String user,
        String group,
        int fileTypeFlags,
        List<PermissionRuleMatcher> rules,
        PackageWriter writer,
        Set<String> parentDirectories
    ) {
        ConfigurableFileTree tree = getObjectFactory().fileTree().from(root);
        tree.include(content.getIncludes().getOrElse(List.of()));
        tree.exclude(content.getExcludes().getOrElse(List.of()));
        boolean ownDirectories = content.getOwnDirectories().getOrElse(false);
        // directories converted to link entries; content below them must be skipped
        Set<String> linkedDirectories = new LinkedHashSet<>();

        tree.visit(new FileVisitor() {
            @Override
            public void visitDir(FileVisitDetails details) {
                if (underLinkedDirectory(details)) {
                    return;
                }
                String path = into + "/" + details.getRelativePath().getPathString();
                String linkTarget = PackagingUtils.relativeLinkTarget(root.toPath(), details.getFile());
                try {
                    if (linkTarget != null) {
                        linkedDirectories.add(details.getRelativePath().getPathString() + "/");
                        writer.addLink(path, linkTarget);
                        return;
                    }
                    if (ownDirectories) {
                        int mode = resolveMode(details, rules, content.getDirMode());
                        if (content.getSetgid().getOrElse(false)) {
                            mode = mode | 02000;
                        }
                        writer.addDirectory(path, mode, user, group);
                        registerParentDirectories(content, path, parentDirectories);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void visitFile(FileVisitDetails details) {
                if (underLinkedDirectory(details)) {
                    return;
                }
                String path = into + "/" + details.getRelativePath().getPathString();
                try {
                    String linkTarget = PackagingUtils.relativeLinkTarget(root.toPath(), details.getFile());
                    if (linkTarget != null) {
                        writer.addLink(path, linkTarget);
                    } else {
                        // the executable-bit workaround also applies on top of explicit modes,
                        // so executables keep their executable bits (e.g. jdk/lib/jexec)
                        int mode = PackagingUtils.getUnixPermission(resolveMode(details, rules, content.getFileMode()), details.getFile());
                        writer.addFile(path, details.getFile(), mode, user, group, fileTypeFlags);
                    }
                    registerParentDirectories(content, path, parentDirectories);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            private boolean underLinkedDirectory(FileVisitDetails details) {
                String relativePath = details.getRelativePath().getPathString();
                return linkedDirectories.stream().anyMatch(relativePath::startsWith);
            }
        });
    }

    private static int resolveMode(FileVisitDetails details, List<PermissionRuleMatcher> rules, Property<Integer> defaultMode) {
        for (PermissionRuleMatcher rule : rules) {
            if (rule.spec().isSatisfiedBy(details)) {
                return rule.mode();
            }
        }
        return defaultMode.getOrElse(PackagingUtils.getUnixPermission(details.getPermissions().toUnixNumeric(), details.getFile()));
    }

    /**
     * Registers every ancestor of {@code path} that is a strict descendant of the mapping's
     * {@code ownParentDirectories} path, so the package manager owns (and removes) the
     * intermediate directories.
     */
    private static void registerParentDirectories(PackageContent content, String path, Set<String> parentDirectories) {
        String below = content.getOwnParentDirectories().getOrNull();
        if (below == null) {
            return;
        }
        String prefix = below.endsWith("/") ? below : below + "/";
        int index = path.lastIndexOf('/');
        while (index > prefix.length()) {
            String parent = path.substring(0, index);
            if (parent.startsWith(prefix) == false) {
                break;
            }
            parentDirectories.add(parent);
            index = parent.lastIndexOf('/');
        }
    }

    private record PermissionRuleMatcher(Spec<FileTreeElement> spec, int mode) {}

    private static List<PermissionRuleMatcher> compileRules(PackageContent content) {
        List<PermissionRuleMatcher> matchers = new ArrayList<>();
        for (PackageContent.PermissionRule rule : content.getPermissionRules().getOrElse(List.of())) {
            PatternSet patternSet = new PatternSet();
            patternSet.include(rule.pattern());
            matchers.add(new PermissionRuleMatcher(patternSet.getAsSpec(), rule.mode()));
        }
        return matchers;
    }

    private static String requireAbsolute(String path) {
        if (path.startsWith("/") == false) {
            throw new IllegalArgumentException("Package destination path must be absolute, got [" + path + "]");
        }
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }
}
