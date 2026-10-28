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
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides distribution-layout copy specs backed by typed inputs.
 */
public class DistributionLayoutExtension {

    private final Project project;
    private final FileSystemOperations fileSystemOperations;
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

    private final Map<String, Map<String, String>> requestedExpansions = new LinkedHashMap<>();

    public DistributionLayoutExtension(
        Project project,
        FileSystemOperations fileSystemOperations,
        ObjectFactory objects,
        ProviderFactory providers
    ) {
        this.project = project;
        this.fileSystemOperations = fileSystemOperations;

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
                LinkedHashMap<String, Map<String, String>> snapshot = new LinkedHashMap<>();
                requestedExpansions.forEach((key, value) -> snapshot.put(key, Map.copyOf(value)));
                return Map.copyOf(snapshot);
            }
        });
    }

    public CopySpec lib(String os, String architecture) {
        CopySpec spec = fileSystemOperations.copySpec();
        for (DistributionLibraries.LibraryEntry entry : DistributionLibraries.entries()) {
            switch (entry.configuration()) {
                case DistributionLibraries.LIBS -> spec.from(configuration(entry.configuration()));
                case DistributionLibraries.LIBS_SERVER_LAUNCHER -> spec.into(entry.targetDirectory(), nested -> {
                    nested.from(configuration(entry.configuration()));
                    for (String nativeConfig : DistributionLibraries.linuxServerLauncherNativeConfigurations(os, architecture)) {
                        nested.from(configuration(nativeConfig));
                    }
                    nested.eachFile(DistributionPermissions.serverLauncher());
                });
                case DistributionLibraries.LIBS_NATIVE -> spec.into(entry.targetDirectory(), nested -> {
                    nested.from(configuration(entry.configuration()));
                    String include = DistributionLibraries.platformInclude(os, architecture);
                    if (include != null) {
                        nested.include(include);
                    }
                });
                default -> spec.into(entry.targetDirectory(), nested -> nested.from(configuration(entry.configuration())));
            }
        }
        return spec;
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

    public Map<String, String> tokenExpansions(String distributionType, boolean testDistro) {
        DistributionType type = DistributionType.fromId(distributionType);
        Map<String, String> expansions = TokenExpansions.forProject(project, type.id(), testDistro);
        synchronized (requestedExpansions) {
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
}
