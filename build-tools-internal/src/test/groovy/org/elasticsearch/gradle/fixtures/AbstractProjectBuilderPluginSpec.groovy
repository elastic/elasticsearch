/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.fixtures

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder
import spock.lang.Specification

/**
 * Shared fixture base for unit tests that cover a Gradle plugin by applying it to a
 * {@link ProjectBuilder} project.
 *
 * <p>Subclasses declare the plugin they exercise via {@link #getPluginClassUnderTest()} so the
 * coverage ArchUnit spec can distinguish real ProjectBuilder-backed plugin tests from plain unit
 * tests that happen to share the plugin's name.
 *
 * <p>The helpers here intentionally stay lightweight: they remove repetitive ProjectBuilder setup
 * and provide a minimal Elasticsearch repository layout for plugins that consult
 * {@code build-tools-internal/version.properties} and {@code server/.../Version.java} during
 * application, without hiding plugin-specific fixture wiring inside a large opaque test harness.
 */
abstract class AbstractProjectBuilderPluginSpec extends Specification {

    private static final String DEFAULT_VERSION_PROPERTIES = """
        elasticsearch      = 9.1.0
        lucene             = 10.2.2
        bundled_jdk_vendor = openjdk
        bundled_jdk        = 24+36@1f9ff9062db4449d8ca828c504ffae90
        minimumJdkVersion  = 21
        minimumRuntimeJava = 21
        minimumCompilerJava = 21
    """.stripIndent()

    private static final String DEFAULT_VERSION_JAVA = """
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
    """.stripIndent()

    abstract <T extends Plugin> Class<T> getPluginClassUnderTest()

    protected final Project buildProject(String name, Project parent = null, File projectDir = null) {
        ProjectBuilder builder = ProjectBuilder.builder().withName(name)
        if (parent != null) {
            builder.withParent(parent)
        }
        if (projectDir != null) {
            builder.withProjectDir(projectDir)
        }
        return builder.build()
    }

    protected final void applyPluginUnderTest(Project project) {
        project.pluginManager.apply(getPluginClassUnderTest())
    }

    protected final void writeMinimalElasticsearchRepoLayout(File workspace) {
        writeBuildToolsVersionProperties(workspace)
        writeVersionJava(workspace)
    }

    protected final void writeBuildToolsVersionProperties(File workspace, String content = DEFAULT_VERSION_PROPERTIES) {
        File buildToolsInternalDir = new File(workspace, "build-tools-internal")
        buildToolsInternalDir.mkdirs()
        new File(buildToolsInternalDir, "version.properties").text = content
    }

    protected final void writeVersionJava(File workspace, String content = DEFAULT_VERSION_JAVA) {
        File versionFileDir = new File(workspace, "server/src/main/java/org/elasticsearch")
        versionFileDir.mkdirs()
        new File(versionFileDir, "Version.java").text = content
    }
}
