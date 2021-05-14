/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.tools.zip.ZipEntry
import org.apache.tools.zip.ZipFile
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Unroll

class InternalDistributionArchiveSetupPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        buildFile << """
        import org.elasticsearch.gradle.internal.SymbolicLinkPreservingTar

        plugins {
            id 'elasticsearch.internal-distribution-archive-setup'
        }
        """
        file('someFile.txt') << "some content"
    }

    def "applies defaults to tar tasks"() {
        given:
        file('someFile.txt') << "some content"
        buildFile << """
        tasks.register('${buildTaskName}', SymbolicLinkPreservingTar) {
            from 'someFile.txt'
        }
        """

        when:
        def result = gradleRunner(buildTaskName).build()

        then:
        file(expectedOutputArchivePath).exists()
        assertTarPermissionDefaults(file(expectedOutputArchivePath))
        assertEmptyDirTasksTriggered(result)

        where:
        buildTaskName       | expectedOutputArchivePath
        "buildDarwinTar"    | "darwin-tar/build/distributions/elasticsearch.tar.gz"
        "buildOssDarwinTar" | "oss-darwin-tar/build/distributions/elasticsearch-oss.tar.gz"
    }

    def "applies defaults to zip tasks"() {
        given:
        buildFile << """
        tasks.register('${buildTaskName}', Zip) {
            from 'someFile.txt'
        }
        """

        when:
        def result = gradleRunner(buildTaskName).build()

        then:
        file(expectedOutputArchivePath).exists()
        assertZipPermissionDefaults(file(expectedOutputArchivePath))
        assertEmptyDirTasksTriggered(result)

        where:
        buildTaskName       | expectedOutputArchivePath
        "buildDarwinZip"    | "darwin-zip/build/distributions/elasticsearch.zip"
        "buildOssDarwinZip" | "oss-darwin-zip/build/distributions/elasticsearch-oss.zip"
    }

    def "registered distribution provides archives and directory variant"() {
        given:
        file('someFile.txt') << "some content"

        settingsFile << """
            include ':consumer'
            include ':producer-tar'
        """

        buildFile << """
        import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
        import org.gradle.api.internal.artifacts.ArtifactAttributes;

        def snapshotFile = file("snapshot-\${version}.txt")
        snapshotFile << 'some snapshot content'
        distribution_archives {
            producerTar {
                content {
                    project.copySpec {
                        from 'someFile.txt'
                        from snapshotFile
                    }
                }
            }
        }

        project('consumer') { p ->
            configurations {
                consumeArchive {}
                consumeDir {}
            }

            dependencies {
                consumeDir project(path: ':producer-tar', configuration:'extracted')
                consumeArchive project(path: ':producer-tar', configuration:'default' )
            }

            tasks.register("copyDir", Copy) {
                from(configurations.consumeDir)
                into('build/dir')
            }

            tasks.register("copyArchive", Copy) {
                from(configurations.consumeArchive)
                into('build/archives')
            }
        }
        """
        when:
        def result = gradleRunner("copyArchive").build()

        then: "tar task executed and target folder contains plain tar"
        result.task(':buildProducerTar').outcome == TaskOutcome.SUCCESS
        result.task(':consumer:copyArchive').outcome == TaskOutcome.SUCCESS
        file("producer-tar/build/distributions/elasticsearch.tar.gz").exists()
        file("consumer/build/archives/elasticsearch.tar.gz").exists()

        when:
        result = gradleRunner("copyDir", "-Pversion=1.0").build()
        then: "plain copy task executed and target folder contains plain content"
        result.task(':buildProducer').outcome == TaskOutcome.SUCCESS
        result.task(':consumer:copyDir').outcome == TaskOutcome.SUCCESS
        file("producer-tar/build/install/someFile.txt").exists()
        file("producer-tar/build/install/snapshot-1.0.txt").exists()
        file("consumer/build/dir/someFile.txt").exists()

        when:
        gradleRunner("copyDir", "-Pversion=2.0").build()
        then: "old content is cleared out"
        file("producer-tar/build/install/someFile.txt").exists()
        !file("producer-tar/build/install/snapshot-1.0.txt").exists()
        file("producer-tar/build/install/snapshot-2.0.txt").exists()
    }

    def "builds extracted distribution via extractedAssemble"() {
        given:
        file('someFile.txt') << "some content"
        settingsFile << """
            include ':producer-tar'
        """

        buildFile << """
        import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
        import org.gradle.api.internal.artifacts.ArtifactAttributes;

        def snapshotFile = file("snapshot.txt")
        snapshotFile << 'some snapshot content'
        distribution_archives {
            producerTar {
                content {
                    project.copySpec {
                        from 'someFile.txt'
                        from snapshotFile
                    }
                }
            }
        }
        """
        when:
        def result = gradleRunner(":producer-tar:extractedAssemble").build()

        then: "tar task executed and target folder contains plain tar"
        result.task(':buildProducer').outcome == TaskOutcome.SUCCESS

        file("producer-tar/build/install").exists()
        file("producer-tar/build/distributions/elasticsearch.tar.gz").exists() == false
    }

    private static boolean assertTarPermissionDefaults(File tarArchive) {
        TarArchiveInputStream tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(tarArchive)))
        try {
            TarArchiveEntry currentEntry = tarInput.getNextTarEntry()
            while (currentEntry != null) {
                if (currentEntry.isDirectory()) {
                    assertDefaultDirPermissions(currentEntry.getMode())
                } else {
                    assertDefaultFilePermissions(currentEntry.getMode())
                }
                currentEntry = tarInput.getNextTarEntry()
            }
            return true
        } finally {
            tarInput.close()
        }
    }

    private static boolean assertZipPermissionDefaults(File archive) {
        ZipFile zip = new ZipFile(archive)
        try {
            Enumeration<ZipEntry> entries = zip.getEntries()
            while (entries.hasMoreElements()) {
                ZipEntry zipEntry = entries.nextElement()
                if (zipEntry.isDirectory()) {
                    assertDefaultDirPermissions(zipEntry.getUnixMode())
                } else {
                    assertDefaultFilePermissions(zipEntry.getUnixMode())
                }
            }
        } finally {
            zip.close()
        }
        true
    }

    private static boolean assertDefaultDirPermissions(int mode) {
        assert ((mode >> 6) & 07) == 7
        assert ((mode >> 3) & 07) == 5
        assert ((mode >> 0) & 07) == 5
        true
    }

    private static boolean assertDefaultFilePermissions(int mode) {
        assert ((mode >> 6) & 07) == 6
        assert ((mode >> 3) & 07) == 4
        assert ((mode >> 0) & 07) == 4
        true
    }

    private static boolean assertEmptyDirTasksTriggered(BuildResult result) {
        result.task(":createJvmOptionsDir").outcome == TaskOutcome.SUCCESS
        result.task(":createLogsDir").outcome == TaskOutcome.SUCCESS
        result.task(":createPluginsDir").outcome == TaskOutcome.SUCCESS
        true
    }
}
