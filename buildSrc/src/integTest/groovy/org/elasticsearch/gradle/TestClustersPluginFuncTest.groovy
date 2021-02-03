/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.GradleRunner
import spock.lang.IgnoreIf
import spock.lang.Requires
import spock.util.environment.OperatingSystem

import static org.elasticsearch.gradle.fixtures.DistributionDownloadFixture.withMockedDistributionDownload

/**
 * We do not have coverage for the test cluster startup on windows yet.
 * One step at a time...
 * */
@IgnoreIf({ os.isWindows() })
class TestClustersPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
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
        """
    }

    def "test cluster distribution is configured and started"() {
        given:
        buildFile << """
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
        def result = withMockedDistributionDownload(gradleRunner("myTask", '-i')) {
            build()
        }

        then:
        result.output.contains("elasticsearch-keystore script executed!")
        assertEsStdoutContains("myCluster", "Starting Elasticsearch process")
        assertEsStdoutContains("myCluster", "Stopping node")
        assertNoCustomDistro('myCluster')
    }

    def "can declare test cluster in lazy evaluated task configuration block"() {
        given:
        buildFile << """
            tasks.register('myTask', SomeClusterAwareTask) {
                testClusters {
                    myCluster {
                        testDistribution = 'default'
                    }
                }
                useCluster testClusters.myCluster
            }
        """

        when:
        def result = withMockedDistributionDownload(gradleRunner("myTask", '-i')) {
            build()
        }

        then:
        result.output.contains("elasticsearch-keystore script executed!")
        assertEsStdoutContains("myCluster", "Starting Elasticsearch process")
        assertEsStdoutContains("myCluster", "Stopping node")
        assertNoCustomDistro('myCluster')
    }

    def "custom distro folder created for tweaked cluster distribution"() {
        given:
        buildFile << """
            testClusters {
              myCluster {
                testDistribution = 'default'
                extraJarFile(file('${someJar().absolutePath}'))
              }
            }

            tasks.register('myTask', SomeClusterAwareTask) {
                useCluster testClusters.myCluster
            }
        """

        when:
        def result = withMockedDistributionDownload(gradleRunner("myTask", '-i')) {
            build()
        }

        then:
        result.output.contains("elasticsearch-keystore script executed!")
        assertEsStdoutContains("myCluster", "Starting Elasticsearch process")
        assertEsStdoutContains("myCluster", "Stopping node")
        assertCustomDistro('myCluster')
    }

    boolean assertEsStdoutContains(String testCluster, String expectedOutput) {
        assert new File(testProjectDir.root,
                "build/testclusters/${testCluster}-0/logs/es.stdout.log").text.contains(expectedOutput)
        true
    }

    boolean assertCustomDistro(String clusterName) {
        assert customDistroFolder(clusterName).exists()
        true
    }

    boolean assertNoCustomDistro(String clusterName) {
        assert !customDistroFolder(clusterName).exists()
        true
    }

    private File customDistroFolder(String clusterName) {
        new File(testProjectDir.root, "build/testclusters/${clusterName}-0/distro")
    }
}
