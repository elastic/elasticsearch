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
package org.elasticsearch.gradle.precommit

import org.apache.rat.anttasks.Report
import org.apache.rat.anttasks.SubstringLicenseMatcher
import org.apache.rat.license.SimpleLicenseFamily
import org.elasticsearch.gradle.AntTask
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.SkipWhenEmpty
import org.gradle.api.tasks.SourceSet

import java.nio.file.Files

/**
 * Checks files for license headers.
 * <p>
 * This is a port of the apache lucene check
 */
public class LicenseHeadersTask extends AntTask {

    @OutputFile
    File reportFile = new File(project.buildDir, 'reports/licenseHeaders/rat.log')

    /** Allowed license families for this project. */
    @Input
    List<String> approvedLicenses = ['Apache', 'Generated']

    /**
     * Files that should be excluded from the license header check. Use with extreme care, only in situations where the license on the
     * source file is compatible with the codebase but we do not want to add the license to the list of approved headers (to avoid the
     * possibility of inadvertently using the license on our own source files).
     */
    @Input
    List<String> excludes = []

    /**
     * Additional license families that may be found. The key is the license category name (5 characters),
     * followed by the family name and the value list of patterns to search for.
     */
    protected Map<String, String> additionalLicenses = new HashMap<>()

    LicenseHeadersTask() {
        description = "Checks sources for missing, incorrect, or unacceptable license headers"
    }

    /**
     * The list of java files to check. protected so the afterEvaluate closure in the
     * constructor can write to it.
     */
    @InputFiles
    @SkipWhenEmpty
    public List<FileCollection> getJavaFiles() {
        return project.sourceSets.collect({it.allJava})
    }

    /**
     * Add a new license type.
     *
     * The license may be added to the {@link #approvedLicenses} using the {@code familyName}.
     *
     * @param categoryName A 5-character string identifier for the license
     * @param familyName An expanded string name for the license
     * @param pattern A pattern to search for, which if found, indicates a file contains the license
     */
    public void additionalLicense(String categoryName, String familyName, String pattern) {
        if (categoryName.length() != 5) {
            throw new IllegalArgumentException("License category name must be exactly 5 characters, got ${categoryName}");
        }
        additionalLicenses.put(categoryName + familyName, pattern);
    }

    @Override
    protected void runAnt(AntBuilder ant) {
        ant.project.addTaskDefinition('ratReport', Report)
        ant.project.addDataTypeDefinition('substringMatcher', SubstringLicenseMatcher)
        ant.project.addDataTypeDefinition('approvedLicense', SimpleLicenseFamily)

        Files.deleteIfExists(reportFile.toPath())

        // run rat, going to the file
        ant.ratReport(reportFile: reportFile.absolutePath, addDefaultLicenseMatchers: true) {
            for (FileCollection dirSet : javaFiles) {
               for (File dir: dirSet.srcDirs) {
                   // sometimes these dirs don't exist, e.g. site-plugin has no actual java src/main...
                   if (dir.exists()) {
                       ant.fileset(dir: dir, excludes: excludes.join(' '))
                   }
               }
            }

            // BSD 4-clause stuff (is disallowed below)
            // we keep this here, in case someone adds BSD code for some reason, it should never be allowed.
            substringMatcher(licenseFamilyCategory: "BSD4 ",
                             licenseFamilyName:     "Original BSD License (with advertising clause)") {
               pattern(substring: "All advertising materials")
            }

            // Apache
            substringMatcher(licenseFamilyCategory: "AL   ",
                             licenseFamilyName:     "Apache") {
               // Apache license (ES)
               pattern(substring: "Licensed to Elasticsearch under one or more contributor")
               // Apache license (ASF)
               pattern(substring: "Licensed to the Apache Software Foundation (ASF) under")
               // this is the old-school one under some files
               pattern(substring: "Licensed under the Apache License, Version 2.0 (the \"License\")")
            }

            // Generated resources
            substringMatcher(licenseFamilyCategory: "GEN  ",
                             licenseFamilyName:     "Generated") {
               // parsers generated by antlr
               pattern(substring: "ANTLR GENERATED CODE")
            }

            // license types added by the project
            for (Map.Entry<String, String[]> additional : additionalLicenses.entrySet()) {
                String category = additional.getKey().substring(0, 5)
                String family = additional.getKey().substring(5)
                substringMatcher(licenseFamilyCategory: category,
                                 licenseFamilyName: family) {
                    pattern(substring: additional.getValue())
                }
            }

            // approved categories
            for (String licenseFamily : approvedLicenses) {
                approvedLicense(familyName: licenseFamily)
            }
        }

        // check the license file for any errors, this should be fast.
        boolean zeroUnknownLicenses = false
        boolean foundProblemsWithFiles = false
        reportFile.eachLine('UTF-8') { line ->
            if (line.startsWith("0 Unknown Licenses")) {
                zeroUnknownLicenses = true
            }

            if (line.startsWith(" !")) {
                foundProblemsWithFiles = true
            }
        }

        if (zeroUnknownLicenses == false || foundProblemsWithFiles) {
            // print the unapproved license section, usually its all you need to fix problems.
            int sectionNumber = 0
            reportFile.eachLine('UTF-8') { line ->
                if (line.startsWith("*******************************")) {
                    sectionNumber++
                } else {
                    if (sectionNumber == 2) {
                        logger.error(line)
                    }
                }
            }
            throw new IllegalStateException("License header problems were found! Full details: " + reportFile.absolutePath)
        }
    }
}
