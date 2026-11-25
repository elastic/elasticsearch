/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

/**
 * Covers the {@code elasticsearch.fwc-test} convention. Its tasks only exist on release branches
 * (a released patch of the current minor plus an unreleased previous minor), so on {@code main} the
 * real build never registers them. This test uses a synthetic 9.1 release-branch version graph so
 * the registration is exercised at PR time.
 */
class FwcTestPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = RestTestBasePlugin

    def setup() {
        versionPropertiesFile.text = versionPropertiesFile.text.replace('elasticsearch     = 9.1.0', 'elasticsearch     = 9.1.1')
        file('server/src/main/java/org/elasticsearch/Version.java').text = """
            package org.elasticsearch;
            public class Version {
                public static final Version V_8_18_0 = new Version(8_18_00_99);
                public static final Version V_9_0_2 = new Version(9_00_02_99);
                public static final Version V_9_0_3 = new Version(9_00_03_99);
                public static final Version V_9_1_0 = new Version(9_01_00_99);
                public static final Version V_9_1_1 = new Version(9_01_01_99);
                public static final Version CURRENT = V_9_1_1;
                private Version(int id) {}
            }
        """.stripIndent()
        // This is an internally consistent synthetic version graph, not historical release data.
        def branches = file('synthetic-branches.json')
        branches.text = """
            {
              "branches": [
                { "branch": "9.1", "version": "9.1.1" },
                { "branch": "9.0", "version": "9.0.3" }
              ]
            }
        """.stripIndent()
        propertiesFile << """
            org.elasticsearch.build.branches-file-location=${branches.absolutePath}
        """
    }

    def "registers fwc tasks as REST integ tests upgrading the previous minor snapshot"() {
        given:
        buildFile << """
            ext.bwc_tests_enabled = true
            apply plugin: 'java'
            apply plugin: 'elasticsearch.fwc-test'

            def fwcTasks = tasks.withType(Test).matching { it.name.endsWith('#fwcTest') }.toList()
            assert fwcTasks*.name == ['v9.1.0#fwcTest']

            def fwcTask = fwcTasks.first()
            assert restTests.tasks*.name.contains(fwcTask.name)
            assert fwcTask.group == 'verification'
            assert fwcTask.systemProperties['tests.old_cluster_version'].toString() == '9.0.3'
            assert fwcTask.systemProperties['tests.new_cluster_version'].toString() == '9.1.0'
            // usesBwcDistribution makes the task depend on a bwc distribution per version. Resolving
            // them would need the real distribution projects, so only check the declared dependencies.
            def distros = fwcTask.dependsOn.findAll { it instanceof org.elasticsearch.gradle.ElasticsearchDistribution }*.name
            assert distros.containsAll(['bwc_9.0.3', 'bwc_9.1.0'])
        """

        when:
        def result = gradleRunner('help').build()

        then:
        result.task(':help').outcome == TaskOutcome.SUCCESS
    }
}
