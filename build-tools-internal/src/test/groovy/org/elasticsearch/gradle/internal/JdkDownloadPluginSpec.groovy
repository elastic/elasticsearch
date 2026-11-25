/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractProjectBuilderPluginSpec
import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.Project
import spock.lang.Unroll

class JdkDownloadPluginSpec extends AbstractProjectBuilderPluginSpec {

    @Override
    Class<JdkDownloadPlugin> getPluginClassUnderTest() {
        return JdkDownloadPlugin
    }

    @Unroll
    def "rejects invalid jdk declaration when #description"() {
        given:
        Project project = createProject()

        when:
        createJdk(project, "testjdk", vendor, version, platform, architecture)

        then:
        IllegalArgumentException e = thrown()
        e.message == message

        where:
        description              | vendor    | version     | platform  | architecture | message
        "vendor is missing"     | null      | "11.0.2+33" | "linux"  | "x64"       | "vendor not specified for jdk [testjdk]"
        "vendor is unknown"     | "unknown" | "11.0.2+33" | "linux"  | "x64"       | "unknown vendor [unknown] for jdk [testjdk], must be one of [adoptium, openjdk, zulu]"
        "version is missing"    | "openjdk" | null         | "linux"  | "x64"       | "version not specified for jdk [testjdk]"
        "version is malformed"  | "openjdk" | "badversion" | "linux"  | "x64"       | "malformed version [badversion] for jdk [testjdk]"
        "platform is missing"   | "openjdk" | "11.0.2+33" | null       | "x64"       | "platform not specified for jdk [testjdk]"
        "platform is unknown"   | "openjdk" | "11.0.2+33" | "unknown" | "x64"       | "unknown platform [unknown] for jdk [testjdk], must be one of [darwin, linux, windows, mac]"
        "architecture is missing" | "openjdk" | "11.0.2+33" | "linux"  | null          | "architecture not specified for jdk [testjdk]"
        "architecture is unknown" | "openjdk" | "11.0.2+33" | "linux"  | "unknown"   | "unknown architecture [unknown] for jdk [testjdk], must be one of [aarch64, x64]"
    }

    @Unroll
    def "parses jdk version #version"() {
        given:
        Project project = createProject()

        when:
        Jdk jdk = createJdk(project, "testjdk", "openjdk", version, "linux", "x64")

        then:
        jdk.major == major
        jdk.baseVersion == baseVersion
        jdk.build == build
        jdk.hash == hash

        where:
        version                                          | major | baseVersion  | build | hash
        "27+35@55ce5470a6294008af0057ff4626d0e5"         | "27"  | "27"         | "35"  | "55ce5470a6294008af0057ff4626d0e5"
        "26.0.2+10@818d462d89b645c7a1aad49066c454e5"     | "26"  | "26.0.2"     | "10"  | "818d462d89b645c7a1aad49066c454e5"
        "26.0.2.1+1@0123456789abcdef0123456789abcdef"    | "26"  | "26.0.2.1"   | "1"   | "0123456789abcdef0123456789abcdef"
        "27.0.1.0.2+1@924dbdccfc234bb9b9355d7c6b66034b"  | "27"  | "27.0.1.0.2" | "1"   | "924dbdccfc234bb9b9355d7c6b66034b"
        "11.0.2+33"                                      | "11"  | "11.0.2"     | "33"  | null
    }

    private static Jdk createJdk(Project project, String name, String vendor, String version, String platform, String architecture) {
        NamedDomainObjectContainer<Jdk> jdks = (NamedDomainObjectContainer<Jdk>) project.extensions.getByName("jdks")
        Jdk created = jdks.create(name) { Jdk jdk ->
            if (vendor != null) {
                jdk.vendor = vendor
            }
            if (version != null) {
                jdk.version = version
            }
            if (platform != null) {
                jdk.platform = platform
            }
            if (architecture != null) {
                jdk.architecture = architecture
            }
        }
        created.finalizeValues()
        return created
    }

    private Project createProject() {
        Project rootProject = buildProject("root")
        Project project = buildProject("consumer", rootProject)
        applyPluginUnderTest(project)
        return project
    }
}
