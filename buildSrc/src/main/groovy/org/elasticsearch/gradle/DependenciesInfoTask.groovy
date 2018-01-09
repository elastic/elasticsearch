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

import org.gradle.api.DefaultTask
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.DependencySet
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
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
public class DependenciesInfoTask extends DefaultTask {

    /** Dependencies to gather information from. */
    @Input
    public DependencySet dependencies

    /** Directory to read license files */
    @InputDirectory
    public File licensesDir = new File(project.projectDir, 'licenses')

    @OutputFile
    File outputFile = new File(project.buildDir, "reports/dependencies/dependencies.csv")

    public DependenciesInfoTask() {
        description = 'Create a CSV file with dependencies information.'
    }

    @TaskAction
    public void generateDependenciesInfo() {
        final StringBuilder output = new StringBuilder()

        for (Dependency dependency : dependencies) {
            // Only external dependencies are checked
            if (dependency.group != null && dependency.group.contains("elasticsearch") == false) {
                final String url = createURL(dependency.group, dependency.name, dependency.version)
                final String licenseType = getLicenseType(dependency.group, dependency.name)
                output.append("${dependency.group}:${dependency.name},${dependency.version},${url},${licenseType}\n")
            }
        }
        outputFile.setText(output.toString(), 'UTF-8')
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
        File license

        if (licensesDir.exists()) {
            licensesDir.eachFileMatch({ it ==~ /.*-LICENSE.*/ }) { File file ->
                String prefix = file.name.split('-LICENSE.*')[0]
                if (group.contains(prefix) || name.contains(prefix)) {
                    license = file.getAbsoluteFile()
                }
            }
        }

        if (license) {
            final String content = license.readLines("UTF-8").toString()
            final String spdx = checkSPDXLicense(content)
            if (spdx ==  null) {
                // License has not be identified as SPDX.
                // As we have the license file, we create a Custom entry with the URL to this license file.
                final gitBranch = System.getProperty('build.branch', 'master')
                final String githubBaseURL = "https://raw.githubusercontent.com/elastic/elasticsearch/${gitBranch}/"
                return "Custom;${license.getCanonicalPath().replaceFirst('.*/elasticsearch/', githubBaseURL)}"
            }
            return spdx
        } else {
            return "UNKNOWN"
        }
    }

    /**
     * Check the license content to identify an SPDX license type.
     *
     * @param licenseText LICENSE file content.
     * @return SPDX identifier or null.
     */
    private String checkSPDXLicense(final String licenseText) {
        String spdx = null

        final String APACHE_2_0 = "Apache.*License.*(v|V)ersion 2.0"
        final String BSD_2 = "BSD 2-clause.*License"
        final String CDDL_1_0 = "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE.*Version 1.0"
        final String CDDL_1_1 = "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE.*Version 1.1"
        final String ICU = "ICU License - ICU 1.8.1 and later"
        final String LGPL_3 = "GNU LESSER GENERAL PUBLIC LICENSE.*Version 3"
        final String MIT = "MIT License"
        final String MOZILLA_1_1 = "Mozilla Public License.*Version 1.1"

        switch (licenseText) {
            case ~/.*${APACHE_2_0}.*/:
                spdx = 'Apache-2.0'
                break
            case ~/.*${MIT}.*/:
                spdx = 'MIT'
                break
            case ~/.*${BSD_2}.*/:
                spdx = 'BSD-2-Clause'
                break
            case ~/.*${LGPL_3}.*/:
                spdx = 'LGPL-3.0'
                break
            case ~/.*${CDDL_1_0}.*/:
                spdx = 'CDDL-1.0'
                break
            case ~/.*${CDDL_1_1}.*/:
                spdx = 'CDDL-1.1'
                break
            case ~/.*${ICU}.*/:
                spdx = 'ICU'
                break
            case ~/.*${MOZILLA_1_1}.*/:
                spdx = 'MPL-1.1'
                break
            default:
                break
        }
        return spdx
    }
}
