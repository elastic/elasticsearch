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

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest

class ElasticsearchJavaPluginFuncTest extends AbstractGradleFuncTest {

    def "compatibility options are resolved from from build params minimum runtime version"() {
        when:
        buildFile.text = """
        plugins {
          id 'elasticsearch.global-build-info'
        }
        import org.elasticsearch.gradle.Architecture
        import org.elasticsearch.gradle.info.BuildParams
        BuildParams.init { it.setMinimumRuntimeVersion(JavaVersion.VERSION_1_10) }

        apply plugin:'elasticsearch.java'

        assert compileJava.sourceCompatibility == JavaVersion.VERSION_1_10.toString()     
        assert compileJava.targetCompatibility == JavaVersion.VERSION_1_10.toString()           
        """

        then:
        gradleRunner("help").build()
    }

    def "compile option --release is configured from targetCompatibility"() {
        when:
        buildFile.text = """
            plugins {
             id 'elasticsearch.java'
            }
            
            compileJava.targetCompatibility = "1.10"
            afterEvaluate {
                assert compileJava.options.release.get() == 10
            }
        """
        then:
        gradleRunner("help").build()
    }
}
