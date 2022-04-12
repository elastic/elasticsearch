/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.precommit.DependencyLicensesTask;
import org.elasticsearch.gradle.internal.precommit.LicenseAnalyzer;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.DependencySet;
import org.gradle.api.artifacts.ModuleVersionIdentifier;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.internal.ConventionTask;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

/**
 * A task to gather information about the dependencies and export them into a csv file.
 * <p>
 * The following information is gathered:
 * <ul>
 *     <li>name: name that identifies the library (groupId:artifactId)</li>
 *     <li>version</li>
 *     <li>URL: link to have more information about the dependency.</li>
 *     <li>license: <a href="https://spdx.org/licenses/">SPDX license</a> identifier, custom license or UNKNOWN.</li>
 * </ul>
 */
public class DependenciesInfoTask extends ConventionTask {

    private final DirectoryProperty licensesDir;

    @OutputFile
    private File outputFile;

    private LinkedHashMap<String, String> mappings;

    public Configuration getRuntimeConfiguration() {
        return runtimeConfiguration;
    }

    public void setRuntimeConfiguration(Configuration runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
    }

    public Configuration getCompileOnlyConfiguration() {
        return compileOnlyConfiguration;
    }

    public void setCompileOnlyConfiguration(Configuration compileOnlyConfiguration) {
        this.compileOnlyConfiguration = compileOnlyConfiguration;
    }

    /**
     * Directory to read license files
     */
    @Optional
    @InputDirectory
    public DirectoryProperty getLicensesDir() {
        return licensesDir;
    }

    public void setLicensesDir(File licensesDir) {
        this.licensesDir.set(licensesDir);
    }

    public File getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }

    /**
     * Dependencies to gather information from.
     */
    @InputFiles
    private Configuration runtimeConfiguration;
    /**
     * We subtract compile-only dependencies.
     */
    @InputFiles
    private Configuration compileOnlyConfiguration;

    @Inject
    public DependenciesInfoTask(ProjectLayout projectLayout, ObjectFactory objectFactory, ProviderFactory providerFactory) {
        this.licensesDir = objectFactory.directoryProperty();
        this.licensesDir.convention(
            providerFactory.provider(() -> projectLayout.getProjectDirectory().dir("licenses"))
                .map(dir -> dir.getAsFile().exists() ? dir : null)
        );
        this.outputFile = projectLayout.getBuildDirectory().dir("reports/dependencies").get().file("dependencies.csv").getAsFile();
        setDescription("Create a CSV file with dependencies information.");
    }

    @TaskAction
    public void generateDependenciesInfo() throws IOException {
        final DependencySet runtimeDependencies = runtimeConfiguration.getAllDependencies();
        // we have to resolve the transitive dependencies and create a group:artifactId:version map

        final Set<String> compileOnlyArtifacts = compileOnlyConfiguration.getResolvedConfiguration()
            .getResolvedArtifacts()
            .stream()
            .map(r -> {
                ModuleVersionIdentifier id = r.getModuleVersion().getId();
                return id.getGroup() + ":" + id.getName() + ":" + id.getVersion();
            })
            .collect(Collectors.toSet());

        final StringBuilder output = new StringBuilder();
        for (final Dependency dep : runtimeDependencies) {
            // we do not need compile-only dependencies here
            if (compileOnlyArtifacts.contains(dep.getGroup() + ":" + dep.getName() + ":" + dep.getVersion())) {
                continue;
            }

            // only external dependencies are checked
            if (dep instanceof ProjectDependency) {
                continue;
            }

            final String url = createURL(dep.getGroup(), dep.getName(), dep.getVersion());
            final String dependencyName = DependencyLicensesTask.getDependencyName(getMappings(), dep.getName());
            getLogger().info("mapped dependency " + dep.getGroup() + ":" + dep.getName() + " to " + dependencyName + " for license info");

            final String licenseType = getLicenseType(dep.getGroup(), dependencyName);
            output.append(dep.getGroup() + ":" + dep.getName() + "," + dep.getVersion() + "," + url + "," + licenseType + "\n");
        }

        Files.write(outputFile.toPath(), output.toString().getBytes("UTF-8"), StandardOpenOption.CREATE);
    }

    @Input
    public LinkedHashMap<String, String> getMappings() {
        return mappings;
    }

    public void setMappings(LinkedHashMap<String, String> mappings) {
        this.mappings = mappings;
    }

    /**
     * Create an URL on <a href="https://repo1.maven.org/maven2/">Maven Central</a>
     * based on dependency coordinates.
     */
    protected String createURL(final String group, final String name, final String version) {
        final String baseURL = "https://repo1.maven.org/maven2";
        return baseURL + "/" + group.replaceAll("\\.", "/") + "/" + name + "/" + version;
    }

    /**
     * Read the LICENSE file associated with the dependency and determine a license type.
     * <p>
     * The license type is one of the following values:
     * <ul>
     * <li><em>UNKNOWN</em> if LICENSE file is not present for this dependency.</li>
     * <li><em>one SPDX identifier</em> if the LICENSE content matches with an SPDX license.</li>
     * <li><em>Custom;URL</em> if it's not an SPDX license,
     * URL is the Github URL to the LICENSE file in elasticsearch repository.</li>
     * </ul>
     *
     * @param group dependency group
     * @param name  dependency name
     * @return SPDX identifier, UNKNOWN or a Custom license
     */
    protected String getLicenseType(final String group, final String name) throws IOException {
        final File license = getDependencyInfoFile(group, name, "LICENSE");
        String licenseType;

        final LicenseAnalyzer.LicenseInfo licenseInfo = LicenseAnalyzer.licenseType(license);
        if (licenseInfo.spdxLicense() == false) {
            // License has not be identified as SPDX.
            // As we have the license file, we create a Custom entry with the URL to this license file.
            final String gitBranch = System.getProperty("build.branch", "main");
            final String githubBaseURL = "https://raw.githubusercontent.com/elastic/elasticsearch/" + gitBranch + "/";
            licenseType = licenseInfo.identifier()
                + ";"
                + license.getCanonicalPath().replaceFirst(".*/elasticsearch/", githubBaseURL)
                + ",";
        } else {
            licenseType = licenseInfo.identifier() + ",";
        }

        if (licenseInfo.sourceRedistributionRequired()) {
            final File sources = getDependencyInfoFile(group, name, "SOURCES");
            licenseType += Files.readString(sources.toPath()).trim();
        }

        return licenseType;
    }

    protected File getDependencyInfoFile(final String group, final String name, final String infoFileSuffix) {
        java.util.Optional<File> license = licensesDir.map(
            licenseDir -> Arrays.stream(
                licenseDir.getAsFile().listFiles((dir, fileName) -> Pattern.matches(".*-" + infoFileSuffix + ".*", fileName))
            ).filter(file -> {
                String prefix = file.getName().split("-" + infoFileSuffix + ".*")[0];
                return group.contains(prefix) || name.contains(prefix);
            }).findFirst()
        ).get();

        return license.orElseThrow(
            () -> new IllegalStateException(
                "Unable to find "
                    + infoFileSuffix
                    + " file for dependency "
                    + group
                    + ":"
                    + name
                    + " in "
                    + licensesDir.getAsFile().getOrNull()
            )
        );
    }
}
