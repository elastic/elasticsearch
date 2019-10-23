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
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.DependencySet
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
            if (dependency.group != null && dependency.group.contains("org.elasticsearch")) {
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
        File license

        if (licensesDir != null) {
            licensesDir.eachFileMatch({ it ==~ /.*-LICENSE.*/ }) { File file ->
                String prefix = file.name.split('-LICENSE.*')[0]
                if (group.contains(prefix) || name.contains(prefix)) {
                    license = file.getAbsoluteFile()
                }
            }
        }

        if (license) {
            // replace * because they are sometimes used at the beginning lines as if the license was a multi-line comment
            final String content = new String(license.readBytes(), "UTF-8").replaceAll("\\s+", " ").replaceAll("\\*", " ")
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

        final String APACHE_2_0 = "Apache.*License.*(v|V)ersion.*2\\.0"

        final String BSD_2 = """
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

 1\\. Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer\\.
 2\\. Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution\\.

THIS SOFTWARE IS PROVIDED BY .+ (``|''|")AS IS(''|") AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED\\.
IN NO EVENT SHALL .+ BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES \\(INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION\\) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
\\(INCLUDING NEGLIGENCE OR OTHERWISE\\) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE\\.
""".replaceAll("\\s+", "\\\\s*")

        final String BSD_3 = """
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

 (1\\.)? Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer\\.
 (2\\.)? Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution\\.
 ((3\\.)? The name of .+ may not be used to endorse or promote products
    derived from this software without specific prior written permission\\.|
  (3\\.)? Neither the name of .+ nor the names of its
     contributors may be used to endorse or promote products derived from
     this software without specific prior written permission\\.)

THIS SOFTWARE IS PROVIDED BY .+ (``|''|")AS IS(''|") AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED\\.
IN NO EVENT SHALL .+ BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES \\(INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION\\) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
\\(INCLUDING NEGLIGENCE OR OTHERWISE\\) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE\\.
""".replaceAll("\\s+", "\\\\s*")

        final String CDDL_1_0 = "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE.*Version 1.0"
        final String CDDL_1_1 = "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE.*Version 1.1"
        final String ICU = "ICU License - ICU 1.8.1 and later"
        final String LGPL_3 = "GNU LESSER GENERAL PUBLIC LICENSE.*Version 3"

        final String MIT = """
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files \\(the "Software"\\), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software\\.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT\\. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE\\.
""".replaceAll("\\s+", "\\\\s*")

        final String MOZILLA_1_1 = "Mozilla Public License.*Version 1.1"

        final String MOZILLA_2_0 = "Mozilla\\s*Public\\s*License\\s*Version\\s*2\\.0"

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
            case ~/.*${BSD_3}.*/:
                spdx = 'BSD-3-Clause'
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
            case ~/.*${MOZILLA_2_0}.*/:
                spdx = 'MPL-2.0'
                break
            default:
                break
        }
        return spdx
    }
}
