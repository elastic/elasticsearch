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

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGitAwareGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class InternalBwcGitPluginFuncTest extends AbstractGitAwareGradleFuncTest {

    def setup() {
        internalBuild()
        buildFile << """
            import org.elasticsearch.gradle.Version;
            apply plugin: org.elasticsearch.gradle.internal.InternalBwcGitPlugin  
            
            bwcGitConfig {
                 bwcVersion = project.provider { Version.fromString("8.1.0") }
                 bwcBranch = project.provider { "8.0" }
                 checkoutDir = project.provider{file("build/checkout")}
            }
        """
    }

    def "current repository can be cloned"() {
        when:
        def result = gradleRunner("createClone", '--stacktrace').build()
        then:
        result.task(":createClone").outcome == TaskOutcome.SUCCESS
        file("cloned/build/checkout/build.gradle").exists()
        file("cloned/build/checkout/settings.gradle").exists()
    }

    def "can resolve checkout folder as project artifact"() {
        given:
        settingsFile << "include ':consumer'"
        file("cloned/consumer/build.gradle") << """
            configurations {
                consumeCheckout
            }
            dependencies {
                consumeCheckout project(path:":", configuration: "checkout")
            }
            
            tasks.register("register") {
                dependsOn configurations.consumeCheckout
                doLast {
                    configurations.consumeCheckout.files.each {
                        println "checkoutDir artifact: " + it
                    }
                }
            }
        """
        when:
        def result = gradleRunner(":consumer:register", '--stacktrace', "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin").build()
        then:
        result.task(":checkoutBwcBranch").outcome == TaskOutcome.SUCCESS
        result.task(":consumer:register").outcome == TaskOutcome.SUCCESS
        normalized(result.output).contains("/cloned/build/checkout")
    }
}
