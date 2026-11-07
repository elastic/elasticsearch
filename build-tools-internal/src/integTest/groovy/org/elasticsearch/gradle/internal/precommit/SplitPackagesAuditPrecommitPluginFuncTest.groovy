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
import org.gradle.testkit.runner.TaskOutcome

class SplitPackagesAuditPrecommitPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<SplitPackagesAuditPrecommitPlugin> pluginClassUnderTest = SplitPackagesAuditPrecommitPlugin.class

    def "reports owning project when project build dirs service is available"() {
        given:
        applyProjectBuildDirsSettingsPlugin()
        configureSplitPackagesBuild()

        when:
        def result = gradleRunner(":b:splitPackagesAudit").buildAndFail()

        then:
        result.task(":b:splitPackagesAudit").outcome == TaskOutcome.FAILED
        assertOutputContains(result.getOutput(), "project :a")
        assertOutputMissing(result.getOutput(), "Build directory unknown to gradle")
    }

    def "falls back to a build directory label when project build dirs service is unavailable"() {
        given:
        configureSplitPackagesBuild()

        when:
        def result = gradleRunner(":b:splitPackagesAudit").buildAndFail()

        then:
        result.task(":b:splitPackagesAudit").outcome == TaskOutcome.FAILED
        assertOutputContains(result.getOutput(), "a/build")
        assertOutputMissing(result.getOutput(), "Build directory unknown to gradle")
        assertOutputMissing(result.getOutput(), "project :a")
    }

    private void applyProjectBuildDirsSettingsPlugin() {
        settingsFile.text = settingsFile.text.replace(
            "id 'elasticsearch.java-toolchain'",
            "id 'elasticsearch.java-toolchain'\n            id 'elasticsearch.project-build-dirs'"
        )
    }

    private void configureSplitPackagesBuild() {
        settingsFile << """
        include 'a'
        include 'b'
        """
        buildFile << """
        subprojects {
            apply plugin:'java'
            plugins.apply(org.elasticsearch.gradle.internal.precommit.SplitPackagesAuditPrecommitPlugin)
        }

        project(':b') {
            dependencies {
                implementation files(findProject(':a').sourceSets.main.output.classesDirs)
            }
        }
        """

        file("a/src/main/java/org/acme/splitpkg/FromA.java").text = """
        package org.acme.splitpkg;
        public class FromA {}
        """

        file("b/src/main/java/org/acme/splitpkg/FromB.java").text = """
        package org.acme.splitpkg;
        public class FromB {}
        """
    }
}
