/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest

import spock.lang.IgnoreIf

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.fixtures.AbstractRestResourcesFuncTest
import org.gradle.testkit.runner.TaskOutcome

@IgnoreIf({ os.isWindows() })
class LegacyYamlRestTestPluginFuncTest extends AbstractRestResourcesFuncTest {

    def "yamlRestTest does nothing when there are no tests"() {
        given:
        // RestIntegTestTask not cc compatible due to
        configurationCacheCompatible = false
        buildFile << """
        plugins {
          id 'elasticsearch.legacy-yaml-rest-test'
        }
        """

        when:
        def result = gradleRunner("yamlRestTest").build()

        then:
        result.task(':yamlRestTest').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.NO_SOURCE
    }

    def "yamlRestTest executes and copies api and tests to correct source set"() {
        given:
        // RestIntegTestTask not cc compatible due to
        configurationCacheCompatible = false
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.legacy-yaml-rest-test'

            dependencies {
               yamlRestTestImplementation "junit:junit:4.12"
            }

            // can't actually spin up test cluster from this test
           tasks.withType(Test).configureEach{ enabled = false }

           tasks.register("printYamlRestTestClasspath").configure {
               doLast {
                   println sourceSets.yamlRestTest.runtimeClasspath.asPath
               }
           }
        """
        String api = "foo.json"
        setupRestResources([api])
        addRestTestsToProject(["10_basic.yml"], "yamlRestTest")
        file("src/yamlRestTest/java/MockIT.java") << "import org.junit.Test;class MockIT { @Test public void doNothing() { }}"

        when:
        def result = gradleRunner("yamlRestTest", "printYamlRestTestClasspath").build()

        then:
        result.task(':yamlRestTest').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE

        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + api).exists()
        file("/build/resources/yamlRestTest/rest-api-spec/test/10_basic.yml").exists()
        file("/build/classes/java/yamlRestTest/MockIT.class").exists()

        // check that our copied specs and tests are on the yamlRestTest classpath
        result.output.contains("./build/restResources/yamlSpecs")
        result.output.contains("./build/restResources/yamlTests")

        when:
        result = gradleRunner("yamlRestTest").build()

        then:
        result.task(':yamlRestTest').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE
    }

    def "#type projects are wired into test cluster setup"() {
        given:
        internalBuild()
        localDistroSetup()
        def distroVersion = VersionProperties.getElasticsearch()

        def subProjectBuildFile = subProject(pluginProjectPath)
        subProjectBuildFile << """
            apply plugin: 'elasticsearch.esplugin'
            apply plugin: 'elasticsearch.legacy-yaml-rest-test'

            dependencies {
               yamlRestTestImplementation "junit:junit:4.12"
            }

            esplugin {
                description = 'test plugin'
                classname = 'com.acme.plugin.TestPlugin'
            }

            // for testing purposes only
            configurations.compileOnly.dependencies.clear()

            testClusters {
              yamlRestTest {
                  version = "$distroVersion"
                  testDistribution = 'INTEG_TEST'
              }
            }
        """
        def testFile = new File(subProjectBuildFile.parentFile, 'src/yamlRestTest/java/org/acme/SomeTestIT.java')
        testFile.parentFile.mkdirs()
        testFile << """
        package org.acme;

        import org.junit.Test;

        public class SomeTestIT {
            @Test
            public void someMethod() {
            }
        }
        """

        when:
        def result = gradleRunner("yamlRestTest", "--console", 'plain', '--stacktrace').buildAndFail()

        then:
        result.task(":distribution:archives:integ-test-zip:buildExpanded").outcome == TaskOutcome.SUCCESS
        result.getOutput().contains(expectedInstallLog)

        where:
        type     | pluginProjectPath   | expectedInstallLog
        "plugin" | ":plugins:plugin-a" | "installing 1 plugins in a single transaction"
        "module" | ":modules:module-a" | "Installing 1 modules"
    }

    private void localDistroSetup() {
        settingsFile << """
        include ":distribution:archives:integ-test-zip"
        """
        def distProjectFolder = file("distribution/archives/integ-test-zip")
        file(distProjectFolder, 'current-marker.txt') << "current"

        def elasticPluginScript = file(distProjectFolder, 'src/bin/elasticsearch-plugin')
        elasticPluginScript << """#!/bin/bash
@echo off
echo "Installing plugin \$0"
"""
        assert elasticPluginScript.setExecutable(true)

        def elasticKeystoreScript = file(distProjectFolder, 'src/bin/elasticsearch-keystore')
        elasticKeystoreScript << """#!/bin/bash
@echo off
echo "Installing keystore \$0"
"""
        assert elasticKeystoreScript.setExecutable(true)

        def elasticScript = file(distProjectFolder, 'src/bin/elasticsearch')
        elasticScript << """#!/bin/bash
@echo off
echo "Running elasticsearch \$0"
"""
        assert elasticScript.setExecutable(true)

        file(distProjectFolder, 'src/config/elasticsearch.properties') << "some propes"
        file(distProjectFolder, 'src/config/jvm.options') << """
-Xlog:gc*,gc+age=trace,safepoint:file=logs/gc.log:utctime,level,pid,tags:filecount=32,filesize=64m
-XX:ErrorFile=logs/hs_err_pid%p.log
-XX:HeapDumpPath=data
"""
        file(distProjectFolder, 'build.gradle') << """
            import org.gradle.api.internal.artifacts.ArtifactAttributes;

            apply plugin:'distribution'
            def buildExpanded = tasks.register("buildExpanded", Copy) {
                into("build/local")

                into('es-dummy-dist') {
                    from('src')
                    from('current-marker.txt')
                }
            }

            configurations {
                extracted {
                    attributes {
                          attribute(ArtifactAttributes.ARTIFACT_FORMAT, "directory")
                    }
                }
            }
            artifacts {
                it.add("extracted", buildExpanded)
            }
        """
    }
}
