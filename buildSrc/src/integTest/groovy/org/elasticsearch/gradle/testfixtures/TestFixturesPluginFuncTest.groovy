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

package org.elasticsearch.gradle.testfixtures

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class TestFixturesPluginFuncTest extends AbstractGradleFuncTest {

    def "can run with local fixture"() {
        setup:
        internalBuild()
        buildFile << """
            apply plugin:'java'
            apply plugin:'elasticsearch.test-base'
            apply plugin:'elasticsearch.test.fixtures'
            testFixtures.useFixture()
            
            repositories {
                mavenCentral()
            }
            dependencies {
                testCompile "junit:junit:4.13.1"
                testCompile "commons-io:commons-io:2.6"
            }
            
            dockerCompose {
                // get it working locally on osx environment
                // TODO: double check this works on ci and other machines in general
                environment.put 'PATH', System.getenv()['PATH'] + ":/usr/local/bin"
            }
            
            tasks.named("test").configure {
                environment "TEST_PORT", "\${-> tasks.named('postProcessFixture').get().ext.'test.fixtures.httpd-fixture.tcp.80'}"
            }
        """

        file("src/test/java/org/acme/SomeTests.java") << """
        package org.acme;
        
        import org.apache.commons.io.IOUtils;
        import java.net.URL;
        import java.net.MalformedURLException;
        import java.io.IOException;
        
        public class SomeTests {
            @org.junit.Test 
            public void testUrl() throws MalformedURLException, IOException {
              String url = "http://localhost:" + System.getenv().get("TEST_PORT") + "/";
              String content = IOUtils.toString(new URL(url));
              org.junit.Assert.assertTrue(content.contains("It works!"));
            }
        }
        """
        file("Dockerfile") << "FROM httpd:2.4"
        file("docker-compose.yml") << """
version: '3'
services:
  httpd-fixture:
    build:
      context: .
      args:
        port: 80
    ports:
      - "80"
"""
        when:
        def result = gradleRunner("test").withEnvironment(System.getenv()).build()
        then:
        result.task(":composeUp").outcome == TaskOutcome.SUCCESS
        result.task(":test").outcome == TaskOutcome.SUCCESS
        result.task(":composeDown").outcome == TaskOutcome.SUCCESS
    }
}
