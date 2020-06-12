/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle

import org.elasticsearch.gradle.precommit.DependencyLicensesTask
import org.elasticsearch.gradle.precommit.LicenseAnalyzer
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.DependencySet
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.internal.ConventionTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

/**
 * A task to gather information about the dependencies and export them into a csv file.
 *
 * The following information is gathered:
 * <ul>
 *     <li>name: name that identifies the library (groupId:artifactId)</li>
 *     <li>version</li>
 *     <li>URL: link to have more information about the dependency.</li>
 *     <li>license: <a href="https://spdx.org/licenses/">SPDX license</a> identifier, custom license or UNKNOWN.</li>
 * </ul>
 *
 */
class DependenciesInfoTask extends ConventionTask {

    /** Dependencies to gather information from. */
    @InputFiles
    Configuration runtimeConfiguration

    /** We subtract compile-only dependencies. */
    @InputFiles
    Configuration compileOnlyConfiguration

    /** Directory to read license files */
    @Optional
    @InputDirectory
    File licensesDir = new File(project.projectDir, 'licenses').exists() ? new File(project.projectDir, 'licenses') : null

    @OutputFile
    File outputFile = new File(project.buildDir, "reports/dependencies/dependencies.csv")

    private LinkedHashMap<String, String> mappings

    DependenciesInfoTask() {
        description = 'Create a CSV file with dependencies information.'
    }

    @TaskAction
    void generateDependenciesInfo() {

        final DependencySet runtimeDependencies = runtimeConfiguration.getAllDependencies()
        // we have to resolve the transitive dependencies and create a group:artifactId:version map
        final Set<String> compileOnlyArtifacts =
                compileOnlyConfiguration
                        .getResolvedConfiguration()
                        .resolvedArtifacts
                        .collect { it -> "${it.moduleVersion.id.group}:${it.moduleVersion.id.name}:${it.moduleVersion.id.version}" }

        final StringBuilder output = new StringBuilder()

        for (final Dependency dependency : runtimeDependencies) {
            // we do not need compile-only dependencies here
            if (compileOnlyArtifacts.contains("${dependency.group}:${dependency.name}:${dependency.version}")) {
                continue
            }
            // only external dependencies are checked
            if (dependency instanceof ProjectDependency) {
                continue
            }

            final String url = createURL(dependency.group, dependency.name, dependency.version)
            final String dependencyName = DependencyLicensesTask.getDependencyName(getMappings(), dependency.name)
            logger.info("mapped dependency ${dependency.group}:${dependency.name} to ${dependencyName} for license info")

            final String licenseType = getLicenseType(dependency.group, dependencyName)
            output.append("${dependency.group}:${dependency.name},${dependency.version},${url},${licenseType}\n")

        }
        outputFile.setText(output.toString(), 'UTF-8')
    }

    @Input
    LinkedHashMap<String, String> getMappings() {
        return mappings
    }

    void setMappings(LinkedHashMap<String, String> mappings) {
        this.mappings = mappings
    }

    /**
     * Create an URL on <a href="https://repo1.maven.org/maven2/">Maven Central</a>
     * based on dependency coordinates.
     */
    protected String createURL(final String group, final String name, final String version){
        final String baseURL = 'https://repo1.maven.org/maven2'
        return "${baseURL}/${group.replaceAll('\\.' , '/')}/${name}/${version}"
    }

    /**
     * Read the LICENSE file associated with the dependency and determine a license type.
     *
     * The license type is one of the following values:
     * <u>
     *     <li><em>UNKNOWN</em> if LICENSE file is not present for this dependency.</li>
     *     <li><em>one SPDX identifier</em> if the LICENSE content matches with an SPDX license.</li>
     *     <li><em>Custom;URL</em> if it's not an SPDX license,
     *          URL is the Github URL to the LICENSE file in elasticsearch repository.</li>
     * </ul>
     *
     * @param group dependency group
     * @param name dependency name
     * @return SPDX identifier, UNKNOWN or a Custom license
     */
    protected String getLicenseType(final String group, final String name) {
        File license = getDependencyInfoFile(group, name, 'LICENSE')
        String licenseType

        final LicenseAnalyzer.LicenseInfo licenseInfo = LicenseAnalyzer.licenseType(license)
        if (licenseInfo.spdxLicense == false) {
            // License has not be identified as SPDX.
            // As we have the license file, we create a Custom entry with the URL to this license file.
            final gitBranch = System.getProperty('build.branch', 'master')
            final String githubBaseURL = "https://raw.githubusercontent.com/elastic/elasticsearch/${gitBranch}/"
            licenseType = "${licenseInfo.identifier};${license.getCanonicalPath().replaceFirst('.*/elasticsearch/', githubBaseURL)},"
        } else {
            licenseType = "${licenseInfo.identifier},"
        }

        if (licenseInfo.sourceRedistributionRequired) {
            File sources = getDependencyInfoFile(group, name, 'SOURCES')
            licenseType += "${sources.text.trim()}"
        }

        return licenseType
    }

    protected File getDependencyInfoFile(final String group, final String name, final String infoFileSuffix) {
        File license = null

        if (licensesDir != null) {
            licensesDir.eachFileMatch({ it ==~ /.*-${infoFileSuffix}.*/ }) { File file ->
                String prefix = file.name.split("-${infoFileSuffix}.*")[0]
                if (group.contains(prefix) || name.contains(prefix)) {
                    license = file.getAbsoluteFile()
                }
            }
        }

        if (license == null) {
            throw new IllegalStateException("Unable to find ${infoFileSuffix} file for dependency ${group}:${name} in ${licensesDir}")
        }

        return license
    }
}
