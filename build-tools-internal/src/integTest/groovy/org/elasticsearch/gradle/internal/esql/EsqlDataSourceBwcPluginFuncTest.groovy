/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.esql

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

class EsqlDataSourceBwcPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = EsqlDataSourceBwcPlugin

    def setup() {
        file('server/src/main/java/org/elasticsearch/Version.java').text = """
            package org.elasticsearch;
            public class Version {
                public static final Version V_8_18_0 = new Version(8_18_00_99);
                public static final Version V_9_0_2 = new Version(9_00_02_99);
                public static final Version V_9_0_9 = new Version(9_00_09_99);
                public static final Version V_9_1_0 = new Version(9_01_00_99);
                public static final Version CURRENT = V_9_1_0;
                private Version(int id) {}
            }
        """.stripIndent()
        // This is an internally consistent synthetic version graph, not historical release data.
        def branches = file('synthetic-branches.json')
        branches.text = """
            {
              "branches": [
                { "branch": "main", "version": "9.1.0" },
                { "branch": "9.0", "version": "9.0.9" }
              ]
            }
        """.stripIndent()
        propertiesFile << """
            org.elasticsearch.build.branches-file-location=${branches.absolutePath}
        """
    }

    def "registers isolated coordinator tasks while preserving current tasks"() {
        given:
        buildFile << """
            import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask

            ext.bwc_tests_enabled = true
            apply plugin: 'java'
            apply plugin: 'elasticsearch.internal-java-rest-test'

            sourceSets {
              csvSpecTest {
                java.srcDir 'src/csvSpecTest/java'
              }
            }

            tasks.register('prepareCsvFixture') {
              def marker = layout.buildDirectory.file('fixtures/ready')
              outputs.file(marker)
              doLast {
                marker.get().asFile.parentFile.mkdirs()
                marker.get().asFile.text = 'ready'
              }
            }
            tasks.named('processCsvSpecTestResources') {
              dependsOn 'prepareCsvFixture'
            }

            // The convention must preserve this intentional state.
            tasks.named('javaRestTest') {
              enabled = false
            }

            // Stands in for the owning suites' own csvSpecTests runner, which selects the default
            // distribution itself. The convention must not select it a second time on this task.
            tasks.register('ownerSpecTests', StandaloneRestIntegTestTask) {
              usesDefaultDistribution("owner suite needs the full distribution")
              testClassesDirs = sourceSets.csvSpecTest.output.classesDirs
              classpath = sourceSets.csvSpecTest.runtimeClasspath
            }

            esqlDataSourceBwc {
              minimumVersion '9.0.0'
              sourceSet 'csvSpecTest'
              classFilter 'org.example.OwnerSpecIT'
              coordinators 'old', 'current'
              systemProperty 'tests.owner.setting', 'value'
              exclude 'org.example.ExcludedIT', 'example', 'covered by a specialized suite'
            }

            assert tasks.named('test').get().enabled
            assert tasks.named('javaRestTest').get().enabled == false

            def bwcTasks = tasks.withType(StandaloneRestIntegTestTask).findAll {
              it.name.contains('#esqlDataSourceBwc') && it.name.endsWith('Coordinator') && it.enabled
            }
            assert bwcTasks.empty == false
            // A release build must not pick these up through check -> bwcTestSnapshots; the func
            // test harness builds snapshots, so here they are enabled.
            assert bwcTasks.every { task ->
                  task.testClassesDirs.files == sourceSets.csvSpecTest.output.classesDirs.files &&
                    task.classpath.files == sourceSets.csvSpecTest.runtimeClasspath.files &&
                    task.maxParallelForks == 1 &&
                    task.filter.includePatterns.contains('org.example.OwnerSpecIT') &&
                    task.filter.excludePatterns.contains('org.example.ExcludedIT') &&
                    task.systemProperties['tests.esql.datasource.bwc'] == 'true' &&
                    task.systemProperties['tests.owner.setting'] == 'value' &&
                    task.systemProperties.containsKey('tests.esql.datasource.current_snapshot') &&
                    task.systemProperties.containsKey('tests.esql.datasource.old_snapshot') &&
                    ['old', 'current'].contains(task.systemProperties['tests.esql.datasource.coordinator'])
            }

            // elasticsearch.bwc-test would otherwise select the default distribution for every
            // standalone REST task here, including ownerSpecTests above, which already selects
            // it and would then have its default_distro inputs registered twice. The convention
            // opts out and selects it per task instead.
            assert project.bwcTestApplyDefaultDistribution == false

            def versions = bwcTasks.collect {
              def match = it.name =~ /^v([^#]+)#/
              assert match.find()
              org.elasticsearch.gradle.Version.fromString(match.group(1))
            }
            assert versions.every { it.onOrAfter(org.elasticsearch.gradle.Version.fromString('9.0.0')) }
            assert bwcTasks.find { it.name.startsWith('v9.0.2#') }
              .systemProperties['tests.esql.datasource.old_snapshot'] == 'false'
            assert bwcTasks.find { it.name.startsWith('v9.0.9#') }
              .systemProperties['tests.esql.datasource.old_snapshot'] == 'true'

            def bwcTask = bwcTasks.first()
            assert bwcTask.getDependsOn().any { it.is(sourceSets.csvSpecTest.output) }
            def processResources = tasks.named('processCsvSpecTestResources').get()
            assert processResources.taskDependencies.getDependencies(processResources)
              .any { it.name == 'prepareCsvFixture' }

            def version = (bwcTask.name =~ /^v([^#]+)#/)[0][1]
            def versionAggregate = tasks.named("v\${version}#esqlDataSourceBwc").get()
            def standardBwc = tasks.named("v\${version}#bwcTest").get()
            assert standardBwc.taskDependencies.getDependencies(standardBwc).contains(versionAggregate)
            assert tasks.named('esqlDataSourceBwc').get().taskDependencies
              .getDependencies(tasks.named('esqlDataSourceBwc').get()).contains(versionAggregate)
        """

        when:
        def result = gradleRunner('help').build()

        then:
        result.task(':help').outcome == TaskOutcome.SUCCESS
    }

    def "rejects exclusions without owner and reason"() {
        given:
        buildFile << """
            ext.bwc_tests_enabled = true
            apply plugin: 'java'
            apply plugin: 'elasticsearch.internal-java-rest-test'

            sourceSets {
              csvSpecTest {}
            }

            esqlDataSourceBwc {
              minimumVersion '9.0.0'
              exclude 'org.example.ExcludedIT', '', ''
            }
        """

        when:
        def result = gradleRunner('help').buildAndFail()

        then:
        result.output.contains('must have an owner')
    }
}
