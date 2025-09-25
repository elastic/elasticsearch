/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle

import spock.lang.IgnoreIf
import spock.lang.Unroll
import spock.util.environment.RestoreSystemProperties

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.GradleRunner

import static org.elasticsearch.gradle.fixtures.DistributionDownloadFixture.*

/**
 * We do not have coverage for the test cluster startup on windows yet.
 * One step at a time...
 * */
@IgnoreIf({ os.isWindows() })
class TestClustersPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        // TestClusterPlugin with adding task listeners is not cc compatible
        configurationCacheCompatible = false
        buildFile << """
            import org.elasticsearch.gradle.testclusters.TestClustersAware
            import org.elasticsearch.gradle.testclusters.ElasticsearchCluster
            plugins {
                id 'elasticsearch.testclusters'
            }

            abstract class SomeClusterAwareTask extends DefaultTask implements TestClustersAware {

                private Collection<ElasticsearchCluster> clusters = new HashSet<>();

                @Override
                @Nested
                public Collection<ElasticsearchCluster> getClusters() {
                    return clusters;
                }

                @OutputFile
                Provider<RegularFile> outputFile

                @Inject
                SomeClusterAwareTask(ProjectLayout projectLayout) {
                    outputFile = projectLayout.getBuildDirectory().file("someclusteraware.txt")
                }

                @TaskAction void doSomething() {
                    outputFile.get().getAsFile().text = "done"
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
        assertEsOutputContains("myCluster", "Starting Elasticsearch process")
        assertEsOutputContains("myCluster", "Stopping node")
        assertNoCustomDistro('myCluster')
    }

    @Unroll
    def "test cluster #inputProperty change is detected"() {
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
        def runner = gradleRunner("myTask", '-i', '-g', gradleUserHome)
        def runningClosure = { GradleRunner r -> r.build() }
        withMockedDistributionDownload(runner, runningClosure)
        def result = inputProperty == "distributionClasspath" ?
            withChangedClasspathMockedDistributionDownload(runner, runningClosure) :
            withChangedConfigMockedDistributionDownload(runner, runningClosure)

        then:
        result.output.contains("Task ':myTask' is not up-to-date because:\n  Input property 'clusters.myCluster\$0.nodes.\$0.$inputProperty'")
        result.output.contains("elasticsearch-keystore script executed!")
        assertEsOutputContains("myCluster", "Starting Elasticsearch process")
        assertEsOutputContains("myCluster", "Stopping node")
        assertNoCustomDistro('myCluster')

        where:
        inputProperty << ["distributionClasspath", "distributionFiles"]
    }

    @Unroll
    def "test cluster #pluginType #propertyName change is detected"() {
        given:
        subProject("test-$pluginType") << """
            plugins {
                id 'elasticsearch.esplugin'
            }
            // do not hassle with resolving predefined dependencies
            configurations.compileOnly.dependencies.clear()
            configurations.testImplementation.dependencies.clear()

            esplugin {
                name = 'test-$pluginType'
                classname = 'org.acme.TestModule'
                description = "test $pluginType description"
            }

            version = "1.0"
            group = 'org.acme'
        """
        buildFile << """
            testClusters {
              myCluster {
                testDistribution = 'default'
                $pluginType ':test-$pluginType'
              }
            }

            tasks.register('myTask', SomeClusterAwareTask) {
                useCluster testClusters.myCluster
            }
        """

        when:
        withMockedDistributionDownload(gradleRunner("myTask", '-g', gradleUserHome)) {
            build()
        }
        fileChange.delegate = this
        fileChange.call(this)
        def result = withMockedDistributionDownload(gradleRunner("myTask", '-i', '-g', gradleUserHome)) {
            build()
        }

        then:
        result.output.contains(
            "Task ':myTask' is not up-to-date because:\n" +
                "  Input property 'clusters.myCluster\$0.$propertyName'"
        )
        result.output.contains("elasticsearch-keystore script executed!")
        assertEsOutputContains("myCluster", "Starting Elasticsearch process")
        assertEsOutputContains("myCluster", "Stopping node")

        where:
        pluginType | propertyName         | fileChange
        'module'   | "installedFiles"     |
            { def testClazz -> testClazz.file("test-module/src/main/plugin-metadata/someAddedConfig.txt") << "new resource file" }
        'plugin'   | "installedFiles"     |
            { def testClazz -> testClazz.file("test-plugin/src/main/plugin-metadata/someAddedConfig.txt") << "new resource file" }
        'module'   | "installedClasspath" |
            { def testClazz -> testClazz.file("test-module/src/main/java/SomeClass.java") << "class SomeClass {}" }
        'plugin'   | "installedClasspath" |
            { def testClazz -> testClazz.file("test-plugin/src/main/java/SomeClass.java") << "class SomeClass {}" }
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
        assertEsOutputContains("myCluster", "Starting Elasticsearch process")
        assertEsOutputContains("myCluster", "Stopping node")
        assertNoCustomDistro('myCluster')
    }

    def "custom distro folder created for tweaked cluster distribution"() {
        given:
        buildFile << """
            testClusters {
              myCluster {
                testDistribution = 'default'
                extraJarFiles(files('${someJar().absolutePath}'))
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
        assertEsOutputContains("myCluster", "Starting Elasticsearch process")
        assertEsOutputContains("myCluster", "Stopping node")
        assertCustomDistro('myCluster')
    }

    @RestoreSystemProperties
    def "override jdk usage via ES_JAVA_HOME for known jdk os incompatibilities"() {
        given:

        settingsFile.text = """
            plugins {
                id 'org.gradle.toolchains.foojay-resolver-convention' version '1.0.0'
            }
            """ + settingsFile.text

        buildFile << """
            testClusters {
              myCluster {
                testDistribution = 'default'
                version = '8.10.4'
              }
            }

            // Force linux platform to trigger jdk override
            elasticsearch_distributions.forEach { d ->
                d.platform = org.elasticsearch.gradle.ElasticsearchDistribution.Platform.LINUX
            }

            tasks.register('myTask', SomeClusterAwareTask) {
                useCluster testClusters.myCluster
            }
        """
        when:
        def result = withMockedDistributionDownload(
            "8.10.4",
            ElasticsearchDistribution.Platform.LINUX,
            gradleRunner("myTask", '-Dos.name=Linux', '-Dos.version=6.14.0-1015-gcp', '-i')
        ) {
            build()
        }

        then:
        result.output.lines().anyMatch { line -> line.startsWith("Running") && line.split().find { it.startsWith("ES_JAVA_HOME=") }.contains("eclipse_adoptium-17") }
    }

    boolean assertEsOutputContains(String testCluster, String expectedOutput) {
        assert new File(
            testProjectDir.root,
            "build/testclusters/${testCluster}-0/logs/es.out"
        ).text.contains(expectedOutput)
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
