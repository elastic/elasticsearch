/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.precommit.DependencyLicensesTask;
import org.elasticsearch.gradle.internal.precommit.LicenseAnalyzer;
import org.gradle.api.artifacts.ArtifactCollection;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.artifacts.component.ModuleComponentIdentifier;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.internal.ConventionTask;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
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
public abstract class DependenciesInfoTask extends ConventionTask {

    @Inject
    public abstract ProviderFactory getProviderFactory();

    /**
     * We have to use ArtifactCollection instead of ResolvedArtifactResult here as we're running
     * into a an issue in Gradle: https://github.com/gradle/gradle/issues/27582
     */

    @Internal
    abstract Property<ArtifactCollection> getRuntimeArtifacts();

    @Input
    public Provider<Set<ModuleComponentIdentifier>> getRuntimeModules() {
        return mapToModuleComponentIdentifiers(getRuntimeArtifacts().get());
    }

    @Internal
    abstract Property<ArtifactCollection> getCompileOnlyArtifacts();

    @Input
    public Provider<Set<ModuleComponentIdentifier>> getCompileOnlyModules() {
        return mapToModuleComponentIdentifiers(getCompileOnlyArtifacts().get());
    }

    /**
     * We need to track file inputs here from the configurations we inspect to ensure we dont miss any
     * artifact transforms that might be applied and fail due to missing task dependency to jar
     * generating tasks.
     * */
    @InputFiles
    abstract ConfigurableFileCollection getClasspath();

    private Provider<Set<ModuleComponentIdentifier>> mapToModuleComponentIdentifiers(ArtifactCollection artifacts) {
        return getProviderFactory().provider(
            () -> artifacts.getArtifacts()
                .stream()
                .map(r -> r.getId())
                .filter(id -> id instanceof ModuleComponentIdentifier)
                .map(id -> (ModuleComponentIdentifier) id)
                .collect(Collectors.toSet())
        );
    }

    private final DirectoryProperty licensesDir;

    @OutputFile
    private File outputFile;

    private LinkedHashMap<String, String> mappings;

    /**
     * Directory to read license files
     */
    @Optional
    @InputDirectory
    public File getLicensesDir() {
        File asFile = licensesDir.get().getAsFile();
        if (asFile.exists()) {
            return asFile;
        }

        return null;
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

    @Inject
    public DependenciesInfoTask(ProjectLayout projectLayout, ObjectFactory objectFactory, ProviderFactory providerFactory) {
        this.licensesDir = objectFactory.directoryProperty();
        this.licensesDir.convention(providerFactory.provider(() -> projectLayout.getProjectDirectory().dir("licenses")));
        this.outputFile = projectLayout.getBuildDirectory().dir("reports/dependencies").get().file("dependencies.csv").getAsFile();
        setDescription("Create a CSV file with dependencies information.");
    }

    @TaskAction
    public void generateDependenciesInfo() throws IOException {

        final Set<String> compileOnlyIds = getCompileOnlyModules().map(
            set -> set.stream()
                .map(id -> id.getModuleIdentifier().getGroup() + ":" + id.getModuleIdentifier().getName() + ":" + id.getVersion())
                .collect(Collectors.toSet())
        ).get();
        final StringBuilder output = new StringBuilder();
        Map<String, String> mappings = getMappings().get();
        for (final ModuleComponentIdentifier dep : getRuntimeModules().get()) {
            // we do not need compile-only dependencies here
            String moduleName = dep.getModuleIdentifier().getName();
            if (compileOnlyIds.contains(dep.getGroup() + ":" + moduleName + ":" + dep.getVersion())) {
                continue;
            }

            // only external dependencies are checked
            if (dep instanceof ProjectDependency || dep.getGroup().startsWith("org.elasticsearch")) {
                continue;
            }

            final String url = createURL(dep.getGroup(), moduleName, dep.getVersion());
            final String dependencyName = DependencyLicensesTask.getDependencyName(mappings, moduleName);
            getLogger().info("mapped dependency " + dep.getGroup() + ":" + moduleName + " to " + dependencyName + " for license info");

            final String licenseType = getLicenseType(dep.getGroup(), dependencyName);
            output.append(dep.getGroup() + ":" + moduleName + "," + dep.getVersion() + "," + url + "," + licenseType + "\n");
        }

        Files.write(outputFile.toPath(), output.toString().getBytes("UTF-8"), StandardOpenOption.CREATE);
    }

    @Input
    @Optional
    public abstract MapProperty<String, String> getMappings();

    /**
     * Create an URL on <a href="https://repo1.maven.org/maven2/">Maven Central</a>
     * based on dependency coordinates.
     */
    protected String createURL(final String group, final String name, final String version) {
        final String baseURL = "https://repo1.maven.org/maven2";
        return baseURL + "/" + group.replace('.', '/') + "/" + name + "/" + version;
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

    private IllegalStateException missingInfo(String group, String name, String infoFileSuffix, String reason) {
        return new IllegalStateException("Unable to find " + infoFileSuffix + " file for dependency " + group + ":" + name + " " + reason);
    }

    protected File getDependencyInfoFile(final String group, final String name, final String infoFileSuffix) {
        File dir = getLicensesDir();
        if (dir == null) {
            throw missingInfo(group, name, infoFileSuffix, " because license dir is missing at " + licensesDir.getAsFile().get());
        }
        java.util.Optional<File> license = Arrays.stream(
            dir.listFiles((d, fileName) -> Pattern.matches(".*-" + infoFileSuffix + ".*", fileName))
        ).filter(file -> {
            String prefix = file.getName().split("-" + infoFileSuffix + ".*")[0];
            return group.contains(prefix) || name.contains(prefix);
        }).findFirst();

        return license.orElseThrow(() -> missingInfo(group, name, infoFileSuffix, " in " + dir));
    }
}
