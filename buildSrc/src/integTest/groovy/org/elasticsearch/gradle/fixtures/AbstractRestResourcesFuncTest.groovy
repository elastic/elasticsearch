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
