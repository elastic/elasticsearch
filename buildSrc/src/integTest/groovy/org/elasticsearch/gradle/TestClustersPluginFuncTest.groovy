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
import org.gradle.testkit.runner.GradleRunner

import static org.elasticsearch.gradle.fixtures.DistributionDownloadFixture.withMockedDistributionDownload

class TestClustersPluginFuncTest extends AbstractGradleFuncTest {

    def "test cluster distribution is configured and started"() {
        given:
        buildFile << """
            import org.elasticsearch.gradle.testclusters.DefaultTestClustersTask
            plugins {
                id 'elasticsearch.testclusters'
            }

            class SomeClusterAwareTask extends DefaultTestClustersTask {
                @TaskAction void doSomething() {
                    println 'SomeClusterAwareTask executed'
                }
            }

            testClusters {
              myCluster {
                testDistribution = 'default'
              }
            }
            
            tasks.register('myTask', SomeClusterAwareTask) {
                useCluster testClusters.myCluster
            }
        """

        when:
        def result = withMockedDistributionDownload(gradleRunner("myTask", '-i')) { GradleRunner runner ->
            return runner.build()
        }

        then:
        result.output.contains("elasticsearch-keystore script executed!")
        assertEsStdoutContains("myCluster","Starting Elasticsearch process")
        assertEsStdoutContains("myCluster","Stopping node")
    }

    boolean assertEsStdoutContains(String testCluster, String expectedOutput) {
        assert new File(testProjectDir.root, "build/testclusters/${testCluster}-0/logs/es.stdout.log").text.contains(expectedOutput)
        true
    }
}
