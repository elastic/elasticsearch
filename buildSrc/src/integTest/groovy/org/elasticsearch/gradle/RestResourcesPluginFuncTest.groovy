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

import org.elasticsearch.gradle.fixtures.AbstractRestResourcesFuncTest
import org.gradle.testkit.runner.TaskOutcome

class RestResourcesPluginFuncTest extends AbstractRestResourcesFuncTest {

    def "restResources does nothing when there are no tests"() {
        given:
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.java'
            apply plugin: 'elasticsearch.rest-resources'
        """

        String api = "foo.json"
        setupRestResources([api])

        when:
        def result = gradleRunner("copyRestApiSpecsTask").build()

        then:
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE
    }

    def "restResources copies API by default for projects with tests"() {
        given:
        internalBuild()
        buildFile << """
           apply plugin: 'elasticsearch.java'
           apply plugin: 'elasticsearch.rest-resources'
        """
        String api = "foo.json"
        setupRestResources([api])
        addRestTestsToProject(["10_basic.yml"])

        when:
        def result = gradleRunner("copyRestApiSpecsTask").build()

        then:
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE
        file("/build/resources/test/rest-api-spec/api/" + api).exists()
    }

    def "restResources copies all core API (but not x-pack) by default for projects with copied tests"() {
        given:
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.java'
            apply plugin: 'elasticsearch.rest-resources'

            restResources {
                restTests {
                    includeCore 'foo'
                    includeXpack 'bar'
                }
            }
        """
        String apiCore1 = "foo1.json"
        String apiCore2 = "foo2.json"
        String apiXpack = "xpack.json"
        String coreTest = "foo/10_basic.yml"
        String xpackTest = "bar/10_basic.yml"
        setupRestResources([apiCore1, apiCore2], [coreTest], [apiXpack], [xpackTest])
        // intentionally not adding tests to project, they will be copied over via the plugin
        // this tests that the test copy happens before the api copy since the api copy will only trigger if there are tests in the project

        when:
        def result = gradleRunner("copyRestApiSpecsTask").build()

        then:
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.SUCCESS
        file("/build/resources/test/rest-api-spec/api/" + apiCore1).exists()
        file("/build/resources/test/rest-api-spec/api/" + apiCore2).exists()
        file("/build/resources/test/rest-api-spec/api/" + apiXpack).exists() == false //x-pack specs must be explicitly configured
        file("/build/resources/test/rest-api-spec/test/" + coreTest).exists()
        file("/build/resources/test/rest-api-spec/test/" + xpackTest).exists()
    }

    def "restResources copies API by configuration"() {
        given:
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.java'
            apply plugin: 'elasticsearch.rest-resources'

            restResources {
                restApi {
                    includeCore 'foo'
                    includeXpack 'xpackfoo'
                }
            }
        """
        String apiFoo = "foo.json"
        String apiXpackFoo = "xpackfoo.json"
        String apiBar = "bar.json"
        String apiXpackBar = "xpackbar.json"
        setupRestResources([apiFoo, apiBar], [], [apiXpackFoo, apiXpackBar])
        addRestTestsToProject(["10_basic.yml"])

        when:
        def result = gradleRunner("copyRestApiSpecsTask").build()

        then:
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE
        file("/build/resources/test/rest-api-spec/api/" + apiFoo).exists()
        file("/build/resources/test/rest-api-spec/api/" + apiXpackFoo).exists()
        file("/build/resources/test/rest-api-spec/api/" + apiBar).exists() ==false
        file("/build/resources/test/rest-api-spec/api/" + apiXpackBar).exists() == false
    }

    def "restResources copies Tests and API by configuration"() {
        given:
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.java'
            apply plugin: 'elasticsearch.rest-resources'

            restResources {
                restApi {
                    includeCore '*'
                    includeXpack '*'
                }
                restTests {
                    includeCore 'foo'
                    includeXpack 'bar'
                }
            }
        """
        String apiCore1 = "foo1.json"
        String apiCore2 = "foo2.json"
        String apiXpack = "xpack.json"
        String coreTest = "foo/10_basic.yml"
        String xpackTest = "bar/10_basic.yml"
        setupRestResources([apiCore1, apiCore2], [coreTest], [apiXpack], [xpackTest])
        // intentionally not adding tests to project, they will be copied over via the plugin
        // this tests that the test copy happens before the api copy since the api copy will only trigger if there are tests in the project

        when:
        def result = gradleRunner("copyRestApiSpecsTask").build()

        then:
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.SUCCESS
        file("/build/resources/test/rest-api-spec/api/" + apiCore1).exists()
        file("/build/resources/test/rest-api-spec/api/" + apiCore2).exists()
        file("/build/resources/test/rest-api-spec/api/" + apiXpack).exists()
        file("/build/resources/test/rest-api-spec/test/" + coreTest).exists()
        file("/build/resources/test/rest-api-spec/test/" + xpackTest).exists()

        when:
        result = gradleRunner("copyRestApiSpecsTask").build()

        then:
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.UP_TO_DATE
    }
}
