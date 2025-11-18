/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info

import groovy.json.JsonOutput
import spock.lang.Specification
import spock.lang.TempDir

import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.internal.BwcVersions
import org.gradle.api.Project
import org.gradle.api.provider.Provider
import org.gradle.api.provider.ProviderFactory
import org.gradle.testfixtures.ProjectBuilder

import java.nio.file.Path

class GlobalBuildInfoPluginSpec extends Specification {

    @TempDir
    File projectRoot

    Project project

    def setup() {
        project = ProjectBuilder.builder()
            .withProjectDir(projectRoot)
            .withName("bwcTestProject")
            .build()
        project = Spy(project)
        project.getRootProject() >> project

        File buildToolsInternalDir = new File(projectRoot, "build-tools-internal")
        buildToolsInternalDir.mkdirs()
        new File(buildToolsInternalDir, "version.properties").text = """
            elasticsearch     = 9.1.0
            lucene            = 10.2.2

            bundled_jdk_vendor = openjdk
            bundled_jdk = 24+36@1f9ff9062db4449d8ca828c504ffae90
            minimumJdkVersion = 21
            minimumRuntimeJava = 21
            minimumCompilerJava = 21
        """
        File versionFileDir = new File(projectRoot, "server/src/main/java/org/elasticsearch")
        versionFileDir.mkdirs()
        new File(versionFileDir, "Version.java").text = """
            package org.elasticsearch;
            public class Version {
                public static final Version V_8_17_8 = new Version(8_17_08_99);
                public static final Version V_8_18_0 = new Version(8_18_00_99);
                public static final Version V_8_18_1 = new Version(8_18_01_99);
                public static final Version V_8_18_2 = new Version(8_18_02_99);
                public static final Version V_8_18_3 = new Version(8_18_03_99);
                public static final Version V_8_19_0 = new Version(8_19_00_99);
                public static final Version V_9_0_0 = new Version(9_00_00_99);
                public static final Version V_9_0_1 = new Version(9_00_01_99);
                public static final Version V_9_0_2 = new Version(9_00_02_99);
                public static final Version V_9_0_3 = new Version(9_00_03_99);
                public static final Version V_9_1_0 = new Version(9_01_00_99);
                public static final Version CURRENT = V_9_1_0;

            }
        """
    }

    def "resolve unreleased versions from branches file set by Gradle property"() {
        given:
        ProviderFactory providerFactorySpy = Spy(project.getProviders())
        Path branchesJsonPath = projectRoot.toPath().resolve("myBranches.json")
        Provider<String> gradleBranchesLocationProvider = project.providers.provider { return branchesJsonPath.toString() }
        providerFactorySpy.gradleProperty("org.elasticsearch.build.branches-file-location") >> gradleBranchesLocationProvider
        project.getProviders() >> providerFactorySpy
        branchesJsonPath.text = branchesJson(
            [
                new DevelopmentBranch("main", Version.fromString("9.1.0")),
                new DevelopmentBranch("9.0", Version.fromString("9.0.3")),
                new DevelopmentBranch("8.19", Version.fromString("8.19.1")),
                new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            ]
        )

        when:
        project.objects.newInstance(GlobalBuildInfoPlugin).apply(project)
        BuildParameterExtension ext = project.extensions.getByType(BuildParameterExtension)
        BwcVersions bwcVersions = ext.bwcVersions

        then:
        bwcVersions != null
        bwcVersions.unreleased.toSet() == ["9.1.0", "9.0.3", "8.19.1", "8.18.2"].collect { Version.fromString(it) }.toSet()
    }

    String branchesJson(List<DevelopmentBranch> branches) {
        Map<String, Object> branchesFileContent = [
            branches: branches.collect { branch ->
                [
                    branch : branch.name(),
                    version: branch.version().toString(),
                ]
            }
        ]
        return JsonOutput.prettyPrint(JsonOutput.toJson(branchesFileContent))
    }
}
