/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFile;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Sanity checks for a deb or rpm package of Elasticsearch, the package counterpart of the archive checks: the package
 * can be extracted by its package tool, it installs the expected license and notice files, and its metadata declares
 * the expected license.
 *
 * <p>The checks need {@code dpkg-deb} or {@code rpm}, which are only installed on some build hosts. Without the tool
 * the checks are skipped and {@code check} does not depend on them, so it does not build the package either.
 */
public class InternalDistributionPackageCheckPlugin implements Plugin<Project> {

    public static final String EXTENSION_NAME = "distributionPackageCheck";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(BasePlugin.class);
        ProviderFactory providers = project.getProviders();
        ProjectLayout layout = project.getLayout();
        TaskContainer tasks = project.getTasks();

        DistributionPackageCheckExtension extension = project.getExtensions()
            .create(EXTENSION_NAME, DistributionPackageCheckExtension.class);
        extension.getToolSearchPath().convention(PackageTools.DEFAULT_SEARCH_PATH);
        Provider<PackageMetadata.Type> packageType = extension.getPackageType();

        Provider<String> tool = providers.of(PackageToolValueSource.class, spec -> {
            spec.getParameters().getExecutable().set(packageType.map(PackageTools::executableName));
            spec.getParameters().getSearchPath().set(extension.getToolSearchPath());
        });
        Spec<Task> toolAvailable = task -> tool.isPresent();

        TaskProvider<ExtractPackageTask> checkExtraction = tasks.register("checkExtraction", ExtractPackageTask.class, t -> {
            t.setDescription("Extracts the package with its package tool, as a sanity check that it can be extracted.");
            t.onlyIf("the package tool is available", toolAvailable);
            t.getPackageFile().set(extension.getPackageFile());
            t.getPackageType().set(packageType);
            t.getExecutable().set(tool);
            t.getDestinationDir()
                .set(layout.getBuildDirectory().dir(packageType.map(type -> "extracted/" + type.extension() + "-extracted")));
        });

        Provider<List<String>> elasticLicense = DistributionChecks.licenseLines(providers, layout, DistributionChecks.ELASTIC_LICENSE_FILE);
        TaskProvider<CheckFileLinesTask> checkLicense = tasks.register("checkLicense", CheckFileLinesTask.class, t -> {
            t.setDescription("Checks the license file installed by the package.");
            t.onlyIf("the package tool is available", toolAvailable);
            t.getCheckedFile().set(extractedFile(checkExtraction, packageType.map(InternalDistributionPackageCheckPlugin::licensePath)));
            t.getExpectedLines().set(packageType.zip(elasticLicense, InternalDistributionPackageCheckPlugin::expectedLicenseLines));
        });

        TaskProvider<CheckFileLinesTask> checkNotice = tasks.register("checkNotice", CheckFileLinesTask.class, t -> {
            t.setDescription("Checks the notice file installed by the package.");
            t.onlyIf("the package tool is available", toolAvailable);
            t.getCheckedFile().set(extractedFile(checkExtraction, providers.provider(() -> "usr/share/elasticsearch/NOTICE.txt")));
            t.getExpectedLines().set(DistributionChecks.EXPECTED_NOTICE_LINES);
        });

        TaskProvider<CheckPackageLicenseMetadataTask> checkLicenseMetadata = tasks.register(
            "checkLicenseMetadata",
            CheckPackageLicenseMetadataTask.class,
            t -> {
                t.setDescription("Checks the license declared in the package metadata.");
                t.onlyIf("the package tool is available", toolAvailable);
                t.getPackageFile().set(extension.getPackageFile());
                t.getPackageType().set(packageType);
                t.getExecutable().set(tool);
                t.getExpectedLicense().set(packageType.map(InternalDistributionPackageCheckPlugin::expectedLicenseMetadata));
            }
        );

        // only depend on the checks if they can run, otherwise check would build the package just to skip them
        Callable<List<Object>> checks = () -> tool.isPresent()
            ? List.of(checkExtraction, checkLicense, checkNotice, checkLicenseMetadata)
            : List.of();
        tasks.named("check").configure(t -> t.dependsOn(checks));
    }

    private static Provider<RegularFile> extractedFile(TaskProvider<ExtractPackageTask> checkExtraction, Provider<String> relativePath) {
        return checkExtraction.flatMap(t -> t.getDestinationDir().file(relativePath));
    }

    /**
     * Where the package installs its license file, relative to the root of the file system.
     */
    static String licensePath(PackageMetadata.Type type) {
        return switch (type) {
            case DEB -> "usr/share/doc/elasticsearch/copyright";
            case RPM -> "usr/share/elasticsearch/LICENSE.txt";
        };
    }

    /**
     * The expected content of the {@link #licensePath license file}: deb packages install a machine-readable debian
     * copyright file with the license text indented, rpm packages the license text itself.
     */
    static List<String> expectedLicenseLines(PackageMetadata.Type type, List<String> licenseLines) {
        return switch (type) {
            case DEB -> {
                List<String> lines = new ArrayList<>();
                lines.add("Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/");
                lines.add("Copyright: Elasticsearch B.V. <info@elastic.co>");
                lines.add("License: Elastic-License");
                licenseLines.forEach(line -> lines.add(" " + line));
                yield lines;
            }
            case RPM -> licenseLines;
        };
    }

    /**
     * The license the package metadata declares, spelled as each package format expects it.
     */
    static String expectedLicenseMetadata(PackageMetadata.Type type) {
        return switch (type) {
            case DEB -> "Elastic-License";
            case RPM -> "Elastic License";
        };
    }
}
