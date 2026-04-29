/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.TaskOutcome

class AbstractTransportVersionFuncTest extends AbstractGradleFuncTest {

    def javaResource(String project, String path, String content) {
        file("${project}/src/main/resources/${path}").withWriter { writer ->
            writer << content
        }
    }

    def javaSource(String project, String packageName, String className, String imports, String content) {
        String packageSlashes = packageName.replace('.', '/')
        file("${project}/src/main/java/${packageSlashes}/${className}.java").withWriter { writer ->
            writer << """
                package ${packageName};
                ${imports}
                public class ${className} {
                    ${content}
                }
            """
        }
    }

    def referableTransportVersion(String name, String ids) {
        javaResource("myserver", "transport/definitions/referable/" + name + ".csv", ids)
    }

    def unreferableTransportVersion(String name, String id) {
        javaResource("myserver", "transport/definitions/unreferable/" + name + ".csv", id)
    }

    def referableAndReferencedTransportVersion(String name, String ids) {
        return referableAndReferencedTransportVersion(name, ids, "Test${name.capitalize()}")
    }

    def referencedTransportVersion(String name) {
        referencedTransportVersion(name, "Test${name.capitalize()}")
    }

    def referencedTransportVersion(String name, String classname) {
        javaSource("myserver", "org.elasticsearch", classname, "", """
            static final TransportVersion usage = TransportVersion.fromName("${name}");
        """)
    }

    def referableAndReferencedTransportVersion(String name, String ids, String classname) {
        referencedTransportVersion(name, classname)
        referableTransportVersion(name, ids)
    }

    def transportVersionUpperBound(String branch, String name, String id) {
        javaResource("myserver", "transport/upper_bounds/" + branch + ".csv","${name},${id}")
    }

    def validateReferencesFails(String project) {
        return gradleRunner(":${project}:validateTransportVersionReferences").buildAndFail()
    }

    def validateResourcesFails() {
        return gradleRunner(":myserver:validateTransportVersionResources").buildAndFail()
    }

    def assertValidateReferencesFailure(BuildResult result, String project, String expectedOutput) {
        result.task(":${project}:validateTransportVersionReferences").outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, expectedOutput)
    }

    def assertValidateResourcesFailure(BuildResult result, String expectedOutput) {
        result.task(":myserver:validateTransportVersionResources").outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, expectedOutput)
    }

    void assertReferableDefinition(String name, String content) {
        File definitionFile = file("myserver/src/main/resources/transport/definitions/referable/${name}.csv")
        assert definitionFile.exists()
        assert definitionFile.text.strip() == content
    }

    void assertReferableDefinitionDoesNotExist(String name) {
        assert file("myserver/src/main/resources/transport/definitions/referable/${name}.csv").exists() == false
    }

    void assertUnreferableDefinition(String name, String content) {
        File definitionFile = file("myserver/src/main/resources/transport/definitions/unreferable/${name}.csv")
        assert definitionFile.exists()
        assert definitionFile.text.strip() == content
    }

    void assertUpperBound(String name, String content) {
        assert file("myserver/src/main/resources/transport/upper_bounds/${name}.csv").text.strip() == content
    }

    void assertNoChanges() {
        String output = execute("git diff")
        assert output.strip().isEmpty() : "Expected no local git changes, but found:${System.lineSeparator()}${output}"
    }

    def setup() {
        configurationCacheCompatible = false
        internalBuild()
        settingsFile << """
            include ':myserver'
            include ':myplugin'
        """
        versionPropertiesFile.text = versionPropertiesFile.text.replace("9.1.0", "9.2.0")

        file("myserver/build.gradle") << """
            apply plugin: 'java-library'
            apply plugin: 'elasticsearch.transport-version-references'
            apply plugin: 'elasticsearch.transport-version-resources'

            tasks.named('generateTransportVersion') {
                currentUpperBoundName = '9.2'
            }
            tasks.named('validateTransportVersionResources') {
                currentUpperBoundName = '9.2'
            }
        """
        referableAndReferencedTransportVersion("existing_91", "8012000")
        referableAndReferencedTransportVersion("older_92", "8122000")
        referableAndReferencedTransportVersion("existing_92", "8123000,8012001")
        unreferableTransportVersion("initial_9.0.0", "8000000")
        unreferableTransportVersion("initial_9.1.0", "8011000")
        unreferableTransportVersion("initial_8.19.7", "7123001")
        transportVersionUpperBound("9.2", "existing_92", "8123000")
        transportVersionUpperBound("9.1", "existing_92", "8012001")
        transportVersionUpperBound("9.0", "initial_9.0.0", "8000000")
        transportVersionUpperBound("8.19", "initial_8.19.7", "7123001")
        javaResource("myserver", "org/elasticsearch/TransportVersions.csv", """
            9.0.0,8000000
            9.1.0,8012001
        """)
        // a mock version of TransportVersion, just here so we can compile Dummy.java et al
        javaSource("myserver", "org.elasticsearch", "TransportVersion", "", """
            public static TransportVersion fromName(String name) {
                return null;
            }
        """)

        file("myplugin/build.gradle") << """
            apply plugin: 'java-library'
            apply plugin: 'elasticsearch.transport-version-references'

            dependencies {
                implementation project(":myserver")
            }
        """

        setupLocalGitRepo()
        String currentBranch = execute("git branch --show-current")
        if (currentBranch.strip().equals("main") == false) {
            // make sure a main branch exists, some CI doesn't have main set as the default branch
            execute("git checkout -b main")
        }
        execute("git checkout -b test")
    }

    void setupLocalGitRepo() {
        execute("git init")
        execute('git config user.email "build-tool@elastic.co"')
        execute('git config user.name "Build tool"')
        file(".gitignore") << """
        .gradle/
        build/
        """.stripIndent()
        execute("git add .")
        execute('git commit -m "Initial"')
    }
}
