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

    def referableAndReferencedTransportVersion(String name, String ids, String classname) {
        javaSource("myserver", "org.elasticsearch", classname, "", """
            static final TransportVersion usage = TransportVersion.fromName("${name}");
        """)
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

    def setup() {
        configurationCacheCompatible = false
        internalBuild()
        settingsFile << """
            include ':myserver'
            include ':myplugin'
        """

        file("myserver/build.gradle") << """
            apply plugin: 'java-library'
            apply plugin: 'elasticsearch.transport-version-references'
            apply plugin: 'elasticsearch.transport-version-resources'
        """
        referableTransportVersion("existing_91", "8012000")
        referableTransportVersion("existing_92", "8123000,8012001")
        unreferableTransportVersion("initial_9_0_0", "8000000")
        transportVersionUpperBound("9.2", "existing_92", "8123000")
        transportVersionUpperBound("9.1", "existing_92", "8012001")
        // a mock version of TransportVersion, just here so we can compile Dummy.java et al
        javaSource("myserver", "org.elasticsearch", "TransportVersion", "", """
            public static TransportVersion fromName(String name) {
                return null;
            }
        """)
        javaSource("myserver", "org.elasticsearch", "Dummy", "", """
            static final TransportVersion existing91 = TransportVersion.fromName("existing_91");
            static final TransportVersion existing92 = TransportVersion.fromName("existing_92");
        """)

        file("myplugin/build.gradle") << """
            apply plugin: 'java-library'
            apply plugin: 'elasticsearch.transport-version-references'

            dependencies {
                implementation project(":myserver")
            }
        """

        setupLocalGitRepo()
        execute("git checkout -b main")
        execute("git checkout -b test")
    }

    void setupLocalGitRepo() {
        execute("git init")
        execute('git config user.email "build-tool@elastic.co"')
        execute('git config user.name "Build tool"')
        execute("git add .")
        execute('git commit -m "Initial"')
    }
}
