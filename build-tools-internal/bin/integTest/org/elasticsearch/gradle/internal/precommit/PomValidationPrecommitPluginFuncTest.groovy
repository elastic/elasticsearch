/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.internal.conventions.precommit.PomValidationPrecommitPlugin
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.gradle.testkit.runner.TaskOutcome

class PomValidationPrecommitPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends PrecommitPlugin> pluginClassUnderTest = PomValidationPrecommitPlugin.class

    /**
     * Override the parent setup to apply maven-publish and configure a publication
     * BEFORE PomValidationPrecommitPlugin is applied, because the plugin accesses
     * PublishingExtension in its createTask() method.
     */
    def setup() {
        // The parent AbstractGradleInternalPluginFuncTest.setup() already appended
        // the import + plugins block + plugins.apply(...) to buildFile.
        // We need maven-publish applied before the plugin, so we rewrite
        // the build file with the correct ordering.
        def existing = buildFile.text
        // Extract the import line and everything after it
        def importIdx = existing.indexOf("import ")
        def preImport = importIdx >= 0 ? existing.substring(0, importIdx) : ""
        def fromImport = importIdx >= 0 ? existing.substring(importIdx) : existing

        buildFile.text = preImport + """
        import org.elasticsearch.gradle.internal.conventions.precommit.PomValidationPrecommitPlugin

        plugins {
          // bring in build-tools-internal onto the classpath
          id 'elasticsearch.global-build-info'
        }

        apply plugin:'java'
        apply plugin:'maven-publish'

        group = 'org.acme'
        version = '1.0.0'

        publishing {
            publications {
                maven(MavenPublication) {
                    from components.java
                }
            }
        }

        // internally used plugins do not have a plugin id as they are
        // not intended to be used directly from build scripts
        plugins.apply(PomValidationPrecommitPlugin)
        """
    }

    def "detects missing POM elements and reports structured problems"() {
        given:
        // Default POM with no customization will be missing description, url, licenses, developers, scm

        when:
        def result = gradleRunner("validateMavenPom").buildAndFail()

        then:
        result.task(":validateMavenPom").outcome == TaskOutcome.FAILED

        and: "problems report contains pom-validation group"
        assertProblemsReportContains("pom-validation")
        assertProblemsReportContains("elasticsearch-build")
        assertProblemsReportContains("precommit")
        assertProblemsReportHasAtLeast(1)
    }

    def "passes with fully populated POM"() {
        given:
        buildFile << """
        publishing {
            publications {
                maven(MavenPublication) {
                    pom {
                        name = 'Test Project'
                        description = 'A test project'
                        url = 'https://example.com'
                        licenses {
                            license {
                                name = 'Apache License 2.0'
                                url = 'https://www.apache.org/licenses/LICENSE-2.0'
                            }
                        }
                        developers {
                            developer {
                                name = 'Test Dev'
                                url = 'https://example.com/dev'
                            }
                        }
                        scm {
                            url = 'https://example.com/scm'
                        }
                    }
                }
            }
        }
        """

        when:
        def result = gradleRunner("validateMavenPom").build()

        then:
        result.task(":validateMavenPom").outcome == TaskOutcome.SUCCESS
    }

    def "problems have solutions in report"() {
        given:
        // Default POM with missing elements

        when:
        def result = gradleRunner("validateMavenPom").buildAndFail()

        then:
        result.task(":validateMavenPom").outcome == TaskOutcome.FAILED

        and: "each reported problem has a solution"
        def diagnostics = problemsReportDiagnostics()
        diagnostics.every { it.solutions != null && !it.solutions.isEmpty() }
    }
}
