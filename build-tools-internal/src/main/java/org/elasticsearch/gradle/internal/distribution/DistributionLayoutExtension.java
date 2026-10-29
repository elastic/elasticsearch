/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.apache.tools.ant.filters.FixCrLfFilter;
import org.apache.tools.ant.filters.ReplaceTokens;
import org.elasticsearch.gradle.internal.JdkDownloadPlugin;
import org.elasticsearch.gradle.transform.FilteringJarTransform;
import org.gradle.api.Action;
import org.gradle.api.Named;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Bundling;
import org.gradle.api.attributes.Category;
import org.gradle.api.attributes.Usage;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Provides distribution-layout copy specs backed by typed inputs.
 */
public class DistributionLayoutExtension {

    private final Project project;
    private final FileSystemOperations fileSystemOperations;
    private final ObjectFactory objects;
    private final Provider<Map<String, Map<String, String>>> requestedTokenExpansions;

    private final DirectoryProperty sourceDirectory;
    private final ConfigurableFileCollection defaultModules;
    private final ConfigurableFileCollection integTestModules;
    private final ConfigurableFileCollection externalTestModules;
    private final ConfigurableFileCollection systemdModules;
    private final ConfigurableFileCollection defaultConfig;
    private final ConfigurableFileCollection integTestConfig;
    private final ConfigurableFileCollection log4jConfig;
    private final ConfigurableFileCollection defaultBin;
    private final ConfigurableFileCollection integTestBin;
    private final ConfigurableFileCollection defaultNotice;
    private final ConfigurableFileCollection integTestNotice;

    private final List<RegisteredLibrary> registeredLibraries = new ArrayList<>();
    private final Map<String, Map<String, String>> requestedExpansions = new LinkedHashMap<>();

    public DistributionLayoutExtension(
        Project project,
        FileSystemOperations fileSystemOperations,
        ObjectFactory objects,
        ProviderFactory providers
    ) {
        this.project = project;
        this.fileSystemOperations = fileSystemOperations;
        this.objects = objects;

        sourceDirectory = objects.directoryProperty();
        defaultModules = objects.fileCollection();
        integTestModules = objects.fileCollection();
        externalTestModules = objects.fileCollection();
        systemdModules = objects.fileCollection();
        defaultConfig = objects.fileCollection();
        integTestConfig = objects.fileCollection();
        log4jConfig = objects.fileCollection();
        defaultBin = objects.fileCollection();
        integTestBin = objects.fileCollection();
        defaultNotice = objects.fileCollection();
        integTestNotice = objects.fileCollection();

        requestedTokenExpansions = providers.provider(() -> {
            synchronized (requestedExpansions) {
                TreeMap<String, Map<String, String>> snapshot = new TreeMap<>();
                requestedExpansions.forEach((key, value) -> snapshot.put(key, Collections.unmodifiableMap(new TreeMap<>(value))));
                return Collections.unmodifiableMap(snapshot);
            }
        });
    }

    /**
     * Registers one distribution library and eagerly creates its backing resolvable configuration.
     * <p>
     * Keeping registration in the build script lets distribution library source declarations live beside the project path dependency
     * declarations, while this extension applies a shared configuration model and consistent copy-layout behavior.
     * </p>
     */
    public void library(String name, Action<LibrarySpec> action) {
        LibrarySpec spec = new LibrarySpec(name);
        action.execute(spec);
        RegisteredLibrary registered = spec.build();

        createRuntimeLibraryConfiguration(registered.name(), registered.filtered());
        project.getDependencies().add(registered.name(), registered.dependencyNotation());

        if (registered.layout() == DistributionLibraries.Layout.SERVER_LAUNCHER) {
            createRuntimeLibraryConfiguration(registered.nativeX64Configuration(), false);
            createRuntimeLibraryConfiguration(registered.nativeAarch64Configuration(), false);
            project.getDependencies().add(registered.nativeX64Configuration(), registered.nativeX64DependencyNotation());
            project.getDependencies().add(registered.nativeAarch64Configuration(), registered.nativeAarch64DependencyNotation());
        }

        registeredLibraries.add(registered);
    }

    /**
     * Builds the {@code lib/} copy spec.
     *
     * @param reproduceServerLauncherPermissionLeak reproduce the archive-only historical behaviour where lib jars copied
     *     after {@code tools/server-launcher} are 0755 (see the comment below). Archives (tar/zip) pass {@code true};
     *     the deb/rpm packages pass {@code false}, because ospackage applies its own 0644 file mode to those jars.
     */
    public CopySpec lib(String os, String architecture, boolean reproduceServerLauncherPermissionLeak) {
        CopySpec spec = fileSystemOperations.copySpec();
        // Historical behaviour: the original Groovy libFiles copied all libraries into a single copy spec, and the
        // tools/server-launcher entry's eachFile { it.permissions.unix(0755) } leaked the 0755 file mode onto the copy
        // action so that every subsequent into(...) entry inherited 0755 (the archive default is filePermissions(0644)).
        // That makes the plugin/tool/entitlement/platform lib jars 0755 in the shipped ARCHIVES only - the deb/rpm
        // packages ship them 0644 because ospackage imposes its own file mode. The typed
        // DistributionPermissions.serverLauncher() action below does not leak, so we reproduce the leak explicitly here
        // for archives to keep the distribution byte-identical. TODO(follow-up): drop this and normalise these jars to
        // 0644 - see vault note projects/elasticsearch/notes/distribution-lib-jar-permissions-0755-leak.
        boolean leakedServerLauncherPermissions = false;
        for (RegisteredLibrary library : registeredLibraries) {
            boolean inheritExecutable = reproduceServerLauncherPermissionLeak && leakedServerLauncherPermissions;
            switch (library.layout()) {
                case ROOT -> spec.from(configuration(library.name()));
                case NESTED -> spec.into(library.targetDirectory(), nested -> {
                    nested.from(configuration(library.name()));
                    reproduceServerLauncherPermissionLeak(nested, inheritExecutable);
                });
                case SERVER_LAUNCHER -> spec.into(library.targetDirectory(), nested -> {
                    nested.from(configuration(library.name()));
                    String nativeConfiguration = DistributionLibraries.linuxServerLauncherNativeConfiguration(
                        os,
                        architecture,
                        library.nativeX64Configuration(),
                        library.nativeAarch64Configuration()
                    );
                    if (nativeConfiguration != null) {
                        nested.from(configuration(nativeConfiguration));
                    }
                    nested.eachFile(DistributionPermissions.serverLauncher());
                });
                case PLATFORM_NATIVE -> spec.into(library.targetDirectory(), nested -> {
                    nested.from(configuration(library.name()));
                    String include = DistributionLibraries.platformInclude(os, architecture);
                    if (include != null) {
                        nested.include(include);
                    }
                    reproduceServerLauncherPermissionLeak(nested, inheritExecutable);
                });
                default -> throw new IllegalArgumentException("Unexpected distribution library layout [" + library.layout() + "]");
            }
            if (library.layout() == DistributionLibraries.Layout.SERVER_LAUNCHER) {
                leakedServerLauncherPermissions = true;
            }
        }
        return spec;
    }

    private static void reproduceServerLauncherPermissionLeak(CopySpec nested, boolean inheritExecutable) {
        if (inheritExecutable) {
            nested.filePermissions(permissions -> permissions.unix(0755));
        }
    }

    public CopySpec modules(String os, String architecture) {
        List<String> excludedPlatforms = ModulePlatforms.excludedDirectories(os, architecture);
        CopySpec spec = fileSystemOperations.copySpec();
        spec.eachFile(DistributionPermissions.modules(os));
        spec.from(defaultModules, nested -> {
            nested.exclude("**/geo/**");
            for (String excludedPlatform : excludedPlatforms) {
                nested.exclude("**/platform/" + excludedPlatform + "/**");
            }
        });
        spec.from(externalTestModules);
        spec.from(systemdModules);
        return spec;
    }

    public CopySpec integTestModules() {
        CopySpec spec = fileSystemOperations.copySpec();
        spec.from(integTestModules);
        return spec;
    }

    public CopySpec config(String distributionType, boolean testDistro) {
        DistributionType type = DistributionType.fromId(distributionType);
        Map<String, String> expansions = tokenExpansions(type.id(), testDistro);

        CopySpec spec = fileSystemOperations.copySpec();
        spec.with(fileSystemOperations.copySpec(main -> {
            main.from(sourceDirectory.dir("config"));
            main.exclude("log4j2.properties");
            main.filter(Map.of("tokens", expansions), ReplaceTokens.class);
        }));
        spec.from(log4jConfig);
        spec.from(testDistro ? integTestConfig : defaultConfig);
        return spec;
    }

    public CopySpec bin(String distributionType, boolean testDistro) {
        DistributionType type = DistributionType.fromId(distributionType);
        Map<String, String> expansions = tokenExpansions(type.id(), testDistro);

        CopySpec spec = fileSystemOperations.copySpec();
        spec.with(fileSystemOperations.copySpec(nonWindows -> {
            nonWindows.from(sourceDirectory.dir("bin"));
            nonWindows.exclude("*.exe");
            nonWindows.exclude("*.bat");
            nonWindows.eachFile(details -> details.permissions(permissions -> permissions.unix(0755)));
            nonWindows.filter(Map.of("tokens", expansions), ReplaceTokens.class);
        }));

        if (type == DistributionType.ZIP) {
            spec.with(fileSystemOperations.copySpec(windowsBat -> {
                windowsBat.from(sourceDirectory.dir("bin"));
                windowsBat.include("*.bat");
                windowsBat.filter(Map.of("eol", FixCrLfFilter.CrLf.newInstance("crlf")), FixCrLfFilter.class);
                windowsBat.filter(Map.of("tokens", expansions), ReplaceTokens.class);
            }));
            spec.with(fileSystemOperations.copySpec(windowsExe -> {
                windowsExe.from(sourceDirectory.dir("bin"));
                windowsExe.include("*.exe");
            }));
        }

        spec.with(fileSystemOperations.copySpec(moduleBin -> {
            moduleBin.eachFile(details -> details.permissions(permissions -> permissions.unix(0755)));
            moduleBin.from(testDistro ? integTestBin : defaultBin);
            if (type != DistributionType.ZIP) {
                moduleBin.exclude("*.bat");
            }
        }));
        return spec;
    }

    public CopySpec notice(boolean testDistro) {
        CopySpec spec = fileSystemOperations.copySpec();
        if (testDistro) {
            spec.from(integTestNotice);
        } else {
            spec.from(defaultNotice, nested -> nested.filePermissions(permissions -> permissions.unix(0644)));
        }
        return spec;
    }

    public CopySpec jdk(String os, String architecture) {
        Object jdk = JdkDownloadPlugin.getContainer(project).getByName(BundledJdkPlatforms.jdkName(os, architecture));

        CopySpec spec = fileSystemOperations.copySpec();
        spec.from(jdk);
        spec.exclude("demo/**");
        if ("darwin".equals(os)) {
            spec.exclude("Contents/MacOS");
        }
        spec.eachFile(DistributionPermissions.jdk());
        return spec;
    }

    /**
     * Returns token replacements for the requested distribution type and records them in the shared project-level registry.
     * <p>
     * Every {@link org.gradle.api.tasks.AbstractCopyTask} currently consumes this shared registry as an {@code @Input},
     * so tasks can re-run when expansions for an unrelated distribution type change. This is intentional for step 4 and
     * remains more correct than the pre-step-4 behavior where token expansions were not tracked as task inputs.
     * </p>
     */
    public Map<String, String> tokenExpansions(String distributionType, boolean testDistro) {
        DistributionType type = DistributionType.fromId(distributionType);
        Map<String, String> expansions = TokenExpansions.forProject(project, type.id(), testDistro);
        synchronized (requestedExpansions) {
            // TODO(step 7): precise per-task token expansion inputs.
            requestedExpansions.put(type.id() + ":" + testDistro, expansions);
        }
        return expansions;
    }

    Provider<Map<String, Map<String, String>>> getRequestedTokenExpansions() {
        return requestedTokenExpansions;
    }

    public DirectoryProperty getSourceDirectory() {
        return sourceDirectory;
    }

    public ConfigurableFileCollection getDefaultModules() {
        return defaultModules;
    }

    public ConfigurableFileCollection getIntegTestModules() {
        return integTestModules;
    }

    public ConfigurableFileCollection getExternalTestModules() {
        return externalTestModules;
    }

    public ConfigurableFileCollection getSystemdModules() {
        return systemdModules;
    }

    public ConfigurableFileCollection getDefaultConfig() {
        return defaultConfig;
    }

    public ConfigurableFileCollection getIntegTestConfig() {
        return integTestConfig;
    }

    public ConfigurableFileCollection getLog4jConfig() {
        return log4jConfig;
    }

    public ConfigurableFileCollection getDefaultBin() {
        return defaultBin;
    }

    public ConfigurableFileCollection getIntegTestBin() {
        return integTestBin;
    }

    public ConfigurableFileCollection getDefaultNotice() {
        return defaultNotice;
    }

    public ConfigurableFileCollection getIntegTestNotice() {
        return integTestNotice;
    }

    private Object configuration(String name) {
        return project.getConfigurations().getByName(name);
    }

    private void createRuntimeLibraryConfiguration(String name, boolean filtered) {
        project.getConfigurations().create(name, configuration -> {
            configureRuntimeLibraryConfiguration(configuration);
            if (filtered) {
                configuration.getAttributes()
                    .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, FilteringJarTransform.FILTERED_JAR_TYPE);
            }
        });
    }

    private void configureRuntimeLibraryConfiguration(Configuration configuration) {
        configuration.setCanBeConsumed(false);
        configuration.setCanBeResolved(true);
        configuration.attributes(attributes -> {
            attributes.attribute(Category.CATEGORY_ATTRIBUTE, named(Category.class, Category.LIBRARY));
            attributes.attribute(Usage.USAGE_ATTRIBUTE, named(Usage.class, Usage.JAVA_RUNTIME));
            attributes.attribute(Bundling.BUNDLING_ATTRIBUTE, named(Bundling.class, Bundling.EXTERNAL));
        });
    }

    private <T extends Named> T named(Class<T> type, String name) {
        return objects.named(type, name);
    }

    /**
     * Specification of one script-declared distribution library registration.
     */
    public static final class LibrarySpec {

        private final String name;

        private Object dependencyNotation;
        private String targetDirectory;
        private boolean serverLauncher;
        private boolean platformNative;
        private boolean filtered;
        private Object nativeX64DependencyNotation;
        private Object nativeAarch64DependencyNotation;

        private LibrarySpec(String name) {
            this.name = name;
        }

        /**
         * Declares the dependency notation to add to the library configuration.
         */
        public void from(Object dependencyNotation) {
            this.dependencyNotation = dependencyNotation;
        }

        /**
         * Declares the relative target directory under {@code lib/} where this library is copied.
         */
        public void into(String targetDirectory) {
            this.targetDirectory = targetDirectory;
        }

        /**
         * Marks this library as the server-launcher layout, enabling native launcher variants and executable permissions.
         */
        public void serverLauncher() {
            this.serverLauncher = true;
        }

        /**
         * Marks this library as platform-native and applies per-platform include filtering.
         */
        public void platformNative() {
            this.platformNative = true;
        }

        /**
         * Marks this library's configuration to request filtered JAR artifacts.
         */
        public void filtered() {
            this.filtered = true;
        }

        /**
         * Declares the native x64 dependency notation for a server-launcher library.
         */
        public void nativeX64(Object dependencyNotation) {
            this.nativeX64DependencyNotation = dependencyNotation;
        }

        /**
         * Declares the native aarch64 dependency notation for a server-launcher library.
         */
        public void nativeAarch64(Object dependencyNotation) {
            this.nativeAarch64DependencyNotation = dependencyNotation;
        }

        private RegisteredLibrary build() {
            if (dependencyNotation == null) {
                throw new IllegalArgumentException("Missing from(...) declaration for distribution library [" + name + "]");
            }
            if (targetDirectory == null) {
                throw new IllegalArgumentException("Missing into(...) declaration for distribution library [" + name + "]");
            }
            if (serverLauncher && platformNative) {
                throw new IllegalArgumentException(
                    "Distribution library [" + name + "] cannot be both serverLauncher() and platformNative()"
                );
            }
            if (serverLauncher == false && (nativeX64DependencyNotation != null || nativeAarch64DependencyNotation != null)) {
                throw new IllegalArgumentException(
                    "Distribution library [" + name + "] declares native launcher dependencies without serverLauncher()"
                );
            }

            DistributionLibraries.Layout layout = layout();
            if (layout == DistributionLibraries.Layout.SERVER_LAUNCHER) {
                if (nativeX64DependencyNotation == null || nativeAarch64DependencyNotation == null) {
                    throw new IllegalArgumentException(
                        "Distribution library ["
                            + name
                            + "] with serverLauncher() requires both nativeX64(...) and nativeAarch64(...) declarations"
                    );
                }
                return new RegisteredLibrary(
                    name,
                    targetDirectory,
                    layout,
                    filtered,
                    dependencyNotation,
                    name + "NativeX64",
                    name + "NativeAarch64",
                    nativeX64DependencyNotation,
                    nativeAarch64DependencyNotation
                );
            }

            return new RegisteredLibrary(name, targetDirectory, layout, filtered, dependencyNotation, null, null, null, null);
        }

        private DistributionLibraries.Layout layout() {
            if (serverLauncher) {
                return DistributionLibraries.Layout.SERVER_LAUNCHER;
            }
            if (platformNative) {
                return DistributionLibraries.Layout.PLATFORM_NATIVE;
            }
            if (targetDirectory.isEmpty()) {
                return DistributionLibraries.Layout.ROOT;
            }
            return DistributionLibraries.Layout.NESTED;
        }
    }

    private record RegisteredLibrary(
        String name,
        String targetDirectory,
        DistributionLibraries.Layout layout,
        boolean filtered,
        Object dependencyNotation,
        String nativeX64Configuration,
        String nativeAarch64Configuration,
        Object nativeX64DependencyNotation,
        Object nativeAarch64DependencyNotation
    ) {}
}
