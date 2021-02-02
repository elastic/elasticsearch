/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.fixtures;


abstract class AbstractRestResourcesFuncTest extends AbstractGradleFuncTest {

    void setupRestResources(List<String> apis, List<String> tests = [], List<String> xpackApis = [], List<String> xpackTests = []) {
        addSubProject(":test:framework") << "apply plugin: 'elasticsearch.java'"
        addSubProject(":distribution:archives:integ-test-zip") << "configurations { extracted }"
        addSubProject(":rest-api-spec") << """
        configurations { restSpecs\nrestTests }
        artifacts {
          restSpecs(new File(projectDir, "src/main/resources/rest-api-spec/api"))
          restTests(new File(projectDir, "src/main/resources/rest-api-spec/test"))
        }
        """
        addSubProject(":x-pack:plugin") << """
        configurations { restXpackSpecs\nrestXpackTests }
        artifacts {
          //The api and tests need to stay at src/test/... since some external tooling depends on that exact file path.
          restXpackSpecs(new File(projectDir, "src/test/resources/rest-api-spec/api"))
          restXpackTests(new File(projectDir, "src/test/resources/rest-api-spec/test"))
        }
        """

        xpackApis.each { api ->
            file("x-pack/plugin/src/test/resources/rest-api-spec/api/" + api) << ""
        }
        xpackTests.each { test ->
            file("x-pack/plugin/src/test/resources/rest-api-spec/test/" + test) << ""
        }

        apis.each { api ->
            file("rest-api-spec/src/main/resources/rest-api-spec/api/" + api) << ""
        }
        tests.each { test ->
            file("rest-api-spec/src/main/resources/rest-api-spec/test/" + test) << ""
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
