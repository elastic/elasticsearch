/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.fixtures.AbstractProjectBuilderPluginSpec
import org.gradle.api.Plugin
import org.gradle.api.initialization.ProjectDescriptor

class ProjectBuildDirsSettingsPluginTests extends AbstractProjectBuilderPluginSpec {

    @Override
    Class<? extends Plugin> getPluginClassUnderTest() {
        return ProjectBuildDirsSettingsPlugin.class
    }

    def "collectProjectBuildDirs maps the descriptor tree to normalized build directories"() {
        given:
        File rootDir = new File("/tmp/root")
        ProjectDescriptor child = projectDescriptor(":a", new File(rootDir, "a/./"), [] as Set<ProjectDescriptor>)
        ProjectDescriptor root = projectDescriptor(":", rootDir, [child] as Set<ProjectDescriptor>)

        when:
        Map<File, String> buildDirs = ProjectBuildDirsSettingsPlugin.collectProjectBuildDirs(root)

        then:
        File rootBuildDir = new File(rootDir, "build").toPath().normalize().toFile()
        File childBuildDir = new File(rootDir, "a/build").toPath().normalize().toFile()
        buildDirs == [(rootBuildDir): ":", (childBuildDir): ":a"]
    }

    private static ProjectDescriptor projectDescriptor(String path, File projectDir, Set<ProjectDescriptor> children) {
        // ProjectDescriptor instances are created by Gradle during settings initialization and
        // cannot be directly instantiated in unit tests, so this small interface-backed test
        // double only implements the methods the mapping helper reads.
        return [
            getPath: { -> path },
            getProjectDir: { -> projectDir },
            getChildren: { -> children },
        ] as ProjectDescriptor
    }
}
