/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest

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
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + api).exists()
    }

    def "restResources copies all API by default for projects with copied tests"() {
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
        setupRestResources([apiCore1, apiCore2, apiXpack], [coreTest], [xpackTest])
        // intentionally not adding tests to project, they will be copied over via the plugin
        // this tests that the test copy happens before the api copy since the api copy will only trigger if there are tests in the project

        when:
        def result = gradleRunner("copyRestApiSpecsTask").build()

        then:
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.SUCCESS
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + apiCore1).exists()
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + apiCore2).exists()
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + apiXpack).exists()
        file("/build/restResources/yamlTests/rest-api-spec/test/" + coreTest).exists()
        file("/build/restResources/yamlTests/rest-api-spec/test/" + xpackTest).exists()
    }

    def "restResources copies API by configuration"() {
        given:
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.java'
            apply plugin: 'elasticsearch.rest-resources'

            restResources {
                restApi {
                    include 'foo', 'xpackfoo'
                }
            }
        """
        String apiFoo = "foo.json"
        String apiXpackFoo = "xpackfoo.json"
        String apiBar = "bar.json"
        String apiXpackBar = "xpackbar.json"
        setupRestResources([apiFoo, apiBar, apiXpackFoo, apiXpackBar])
        addRestTestsToProject(["10_basic.yml"])

        when:
        def result = gradleRunner("copyRestApiSpecsTask").build()

        then:
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + apiFoo).exists()
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + apiXpackFoo).exists()
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + apiBar).exists() == false
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + apiXpackBar).exists() == false
    }

    def "restResources copies Tests and API by configuration"() {
        given:
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.java'
            apply plugin: 'elasticsearch.rest-resources'

            restResources {
                restApi {
                    include '*'
                }
                restTests {
                    includeCore 'foo'
                    includeXpack 'bar'
                }
            }

            tasks.named("copyYamlTestsTask").configure {
                it.substitutions = [ 'replacedValue' : 'replacedWithValue' ]
            }
        """
        String apiCore1 = "foo1.json"
        String apiCore2 = "foo2.json"
        String apiXpack = "xpack.json"
        String coreTest = "foo/10_basic.yml"
        String xpackTest = "bar/10_basic.yml"
        setupRestResources([apiCore1, apiCore2, apiXpack], [coreTest], [xpackTest])

        // drop a value to replace from expansions above into a test file
        file("rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/" + coreTest) << "@replacedValue@"

        // intentionally not adding tests to project, they will be copied over via the plugin
        // this tests that the test copy happens before the api copy since the api copy will only trigger if there are tests in the project

        when:
        def result = gradleRunner("copyRestApiSpecsTask").build()

        then:
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.SUCCESS
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + apiCore1).exists()
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + apiCore2).exists()
        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + apiXpack).exists()
        file("/build/restResources/yamlTests/rest-api-spec/test/" + coreTest).exists()
        file("/build/restResources/yamlTests/rest-api-spec/test/" + xpackTest).exists()

        // confirm that replacement happened
        file("/build/restResources/yamlTests/rest-api-spec/test/" + coreTest).getText("UTF-8") == "replacedWithValue"

        when:
        result = gradleRunner("copyRestApiSpecsTask").build()

        then:
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.UP_TO_DATE
    }
}
