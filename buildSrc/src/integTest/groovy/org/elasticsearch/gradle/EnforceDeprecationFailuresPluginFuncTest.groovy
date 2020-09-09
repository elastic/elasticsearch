/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle

import org.gradle.testkit.runner.GradleRunner
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.lang.management.ManagementFactory

class EnforceDeprecationFailuresPluginFuncTest extends Specification {

    @Rule
    TemporaryFolder testProjectDir = new TemporaryFolder()

    File settingsFile
    File buildFile

    def setup() {
        settingsFile = testProjectDir.newFile('settings.gradle')
        settingsFile << "rootProject.name = 'hello-world'"
        buildFile = testProjectDir.newFile('build.gradle')
        buildFile << """plugins {
            id 'elasticsearch.enforce-deprecation-use-failures'
        }
        """
    }

    def "fails on testCompile resolution"() {
        given:
        buildFile << """
            apply plugin:'java'
            dependencies {
                compile "org.acme:some-lib:1.0"
            }

            task resolve {
                doLast {
                    configurations.testCompile.resolve()
                }
            }
            """
        when:
        def result = gradleRunner("resolve").buildAndFail()
        then:
        assertOutputContains(result.output, """
* What went wrong:
Execution failed for task ':resolve'.
> Resolving configuration testCompile is no longer supported. Use testImplementation instead.
""")
    }

    def "fails on testCompile dependency declaration"() {
        given:
        buildFile << """
            apply plugin:'java-base'
            sourceSets {
                test
            }

            dependencies {
                testCompile "org.acme:some-lib:1.0"
            }

            task resolve {
                doLast {
                    configurations.testCompile.resolve()
                }
            }
            """
        when:
        def result = gradleRunner("resolve").buildAndFail()
        then:
        assertOutputContains(result.output, """
* What went wrong:
Execution failed for task ':resolve'.
> Declaring dependencies for configuration testCompile is no longer supported. Use testImplementation instead.
""")
    }

    private GradleRunner gradleRunner(String... arguments) {
        GradleRunner.create()
            .withDebug(ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0)
            .withProjectDir(testProjectDir.root)
            .withArguments(arguments)
            .withPluginClasspath()
            .forwardOutput()
    }

    def assertOutputContains(String givenOutput, String expected) {
        assert normalizedString(givenOutput).contains(normalizedString(expected))
        true
    }

    String normalizedString(String input) {
        return input.readLines().join("\n")
    }
}
