/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.fixtures;


abstract class AbstractRestResourcesFuncTest extends AbstractGradleFuncTest {

    def setup() {
        subProject(":test:framework") << "apply plugin: 'elasticsearch.java'"
        subProject(":test:test-clusters") << "apply plugin: 'elasticsearch.java'"
        subProject(":test:yaml-rest-runner") << "apply plugin: 'elasticsearch.java'"

        subProject(":rest-api-spec") << """
        configurations { restSpecs\nrestTests }
        artifacts {
          restSpecs(new File(projectDir, "src/main/resources/rest-api-spec/api"))
          restTests(new File(projectDir, "src/yamlRestTest/resources/rest-api-spec/test"))
        }
        """

        subProject(":x-pack:plugin") << """
        configurations { restXpackSpecs\nrestXpackTests }
        artifacts {
          restXpackTests(new File(projectDir, "src/yamlRestTest/resources/rest-api-spec/test"))
        }
        """

        subProject(":distribution:archives:integ-test-zip") << "configurations { extracted }"
    }

    void setupRestResources(List<String> apis, List<String> tests = [], List<String> xpackTests = []) {

        xpackTests.each { test ->
            file("x-pack/plugin/src/yamlRestTest/resources/rest-api-spec/test/" + test) << ""
        }

        apis.each { api ->
            file("rest-api-spec/src/main/resources/rest-api-spec/api/" + api) << ""
        }
        tests.each { test ->
            file("rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/" + test) << ""
        }
    }

    void addRestTestsToProject(List<String> tests, String sourceSet = "test") {
        // uses the test source set by default, but in practice it would be a custom source set set by another plugin
        File testDir = new File(testProjectDir.root, "src/" + sourceSet + "/resources/rest-api-spec/test")
        testDir.mkdirs();
        tests.each { test ->
            new File(testDir, test).createNewFile()
        }
    }
}
