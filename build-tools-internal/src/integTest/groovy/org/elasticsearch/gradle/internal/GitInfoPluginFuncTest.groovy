/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import spock.lang.IgnoreIf

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest

/**
 * Covers how {@code GitInfoPlugin} resolves and shares git metadata via the per-build {@code GitInfoService}:
 * <ul>
 *     <li>plugins consuming git info ({@code elasticsearch.licensing} here) are applied to subprojects only and must not
 *     reach into the root project, so the build has to pass with isolated projects enabled</li>
 *     <li>git metadata stays a configuration cache input, both when read at execution time and at configuration time</li>
 *     <li>included builds resolve the git root the same way as before: their own {@code .git} first (e.g. the
 *     elasticsearch submodule inside elasticsearch-serverless), otherwise the enclosing elasticsearch workspace
 *     (e.g. {@code build-tools})</li>
 * </ul>
 * Git repositories are faked with the minimal set of files {@code GitInfo} reads, so no git binary is required.
 */
// Ignoring this test on windows due to what appears to be a bug in the gradle testkit runner.
// https://github.com/elastic/elasticsearch/issues/129100
@IgnoreIf({ os.isWindows() })
class GitInfoPluginFuncTest extends AbstractGradleFuncTest {

    private static final String ISOLATED_PROJECTS = "-Dorg.gradle.unsafe.isolated-projects=true"

    private static final String REVISION_1 = "1111111111111111111111111111111111111111"
    private static final String REVISION_2 = "2222222222222222222222222222222222222222"
    private static final String SUBMODULE_REVISION = "3333333333333333333333333333333333333333"

    private static final String ORIGIN = "https://github.com/acme/outer.git"
    private static final String SUBMODULE_ORIGIN = "https://github.com/acme/inner.git"

    def "subprojects share git info without accessing the root project"() {
        given:
        fakeGitRepository(new File(projectDir, ".git"), REVISION_1, ORIGIN)
        gitInfoProbe(subProject(":lib-a"))
        gitInfoProbe(subProject(":lib-b"))

        when:
        def result = gradleRunner("printGitInfo", ISOLATED_PROJECTS).build()

        then:
        result.output.contains("GITINFO :lib-a revision=${REVISION_1} origin=${ORIGIN}")
        result.output.contains("GITINFO :lib-b revision=${REVISION_1} origin=${ORIGIN}")
    }

    def "configuration cache is reused and git changes are picked up when read at execution time"() {
        given:
        def gitDir = new File(projectDir, ".git")
        fakeGitRepository(gitDir, REVISION_1, ORIGIN)
        gitInfoProbe(subProject(":lib-a"))

        when:
        gradleRunner("printGitInfo", ISOLATED_PROJECTS).build()
        def reused = gradleRunner("printGitInfo", ISOLATED_PROJECTS).build()

        then:
        reused.output.contains("Reusing configuration cache.")
        reused.output.contains("GITINFO :lib-a revision=${REVISION_1} origin=${ORIGIN}")

        when:
        new File(gitDir, "refs/heads/main").text = REVISION_2 + "\n"
        def afterCommit = gradleRunner("printGitInfo", ISOLATED_PROJECTS).build()

        then:
        // git info is only read at execution time here, so switching commits or branches must not invalidate the cache
        afterCommit.output.contains("Reusing configuration cache.")
        afterCommit.output.contains("GITINFO :lib-a revision=${REVISION_2} origin=${ORIGIN}")
    }

    def "configuration cache is invalidated when git info read at configuration time changes"() {
        given:
        def gitDir = new File(projectDir, ".git")
        fakeGitRepository(gitDir, REVISION_1, ORIGIN)
        gitInfoProbe(subProject(":lib-a")) << """
            println "CONFIGURED revision=\${gitInfo.get().revision}"
        """

        when:
        gradleRunner("printGitInfo", ISOLATED_PROJECTS).build()
        def reused = gradleRunner("printGitInfo", ISOLATED_PROJECTS).build()

        then:
        reused.output.contains("Reusing configuration cache.")
        reused.output.contains("CONFIGURED") == false

        when:
        new File(gitDir, "refs/heads/main").text = REVISION_2 + "\n"
        def afterCommit = gradleRunner("printGitInfo", ISOLATED_PROJECTS).build()

        then:
        afterCommit.output.contains("Reusing configuration cache.") == false
        afterCommit.output.contains("CONFIGURED revision=${REVISION_2}")
        afterCommit.output.contains("GITINFO :lib-a revision=${REVISION_2} origin=${ORIGIN}")
    }

    def "included builds resolve their own git root or the enclosing elasticsearch workspace"() {
        given:
        // outer repository, e.g. elasticsearch-serverless
        fakeGitRepository(new File(projectDir, ".git"), REVISION_1, ORIGIN)
        gitInfoProbe(subProject(":lib-a"))

        // an included build checked out as git submodule, e.g. the elasticsearch submodule in serverless
        def submoduleGitDir = new File(projectDir, ".git/modules/elasticsearch")
        fakeGitRepository(submoduleGitDir, SUBMODULE_REVISION, SUBMODULE_ORIGIN)
        def submodule = new File(projectDir, "elasticsearch")
        file(submodule, ".git").text = "gitdir: ../.git/modules/elasticsearch\n"
        file(submodule, "build-tools-internal/version.properties").text = versionPropertiesFile.text
        includedBuild(submodule)

        // an included build without its own .git living inside the elasticsearch workspace, e.g. build-tools
        includedBuild(new File(submodule, "build-tools"))

        settingsFile << """
            includeBuild 'elasticsearch'
            includeBuild 'elasticsearch/build-tools'
        """

        when:
        // Since Gradle 8.0 the build path follows the directory hierarchy of included builds, so the build at
        // 'elasticsearch/build-tools' is addressed as ':elasticsearch:build-tools' although both are included from the root
        // settings. See https://docs.gradle.org/current/userguide/upgrading_version_7.html ("Changes to paths of included builds")
        def result = gradleRunner(
            ":lib-a:printGitInfo",
            ":elasticsearch:lib:printGitInfo",
            ":elasticsearch:build-tools:lib:printGitInfo",
            ISOLATED_PROJECTS
        ).build()

        then:
        result.output.contains("GITINFO :lib-a revision=${REVISION_1} origin=${ORIGIN}")
        result.output.contains("GITINFO :elasticsearch:lib revision=${SUBMODULE_REVISION} origin=${SUBMODULE_ORIGIN}")
        result.output.contains("GITINFO :elasticsearch:build-tools:lib revision=${SUBMODULE_REVISION} origin=${SUBMODULE_ORIGIN}")
    }

    /**
     * Creates the subset of a git directory that {@code GitInfo} reads: HEAD, the referenced branch and the origin remote.
     */
    private static void fakeGitRepository(File gitDir, String revision, String origin) {
        gitDir.mkdirs()
        new File(gitDir, "HEAD").text = "ref: refs/heads/main\n"
        def ref = new File(gitDir, "refs/heads/main")
        ref.parentFile.mkdirs()
        ref.text = revision + "\n"
        new File(gitDir, "config").text = """\
            [core]
            \trepositoryformatversion = 0
            [remote "origin"]
            \turl = ${origin}
            \tfetch = +refs/heads/*:refs/remotes/origin/*
        """.stripIndent()
    }

    /**
     * Creates a standalone build with a single {@code :lib} project applying the git info probe.
     */
    private void includedBuild(File buildDir) {
        file(buildDir, "settings.gradle").text = """
            rootProject.name = '${buildDir.name}'
            include 'lib'
        """
        file(buildDir, "build.gradle").text = ""
        gitInfoProbe(file(buildDir, "lib/build.gradle"))
    }

    /**
     * Applies {@code elasticsearch.licensing}, which applies {@code GitInfoPlugin} to the same project, and registers a
     * {@code printGitInfo} task that resolves git info at execution time. Exposes {@code gitInfo} for further use in the
     * build script.
     */
    private static File gitInfoProbe(File buildFile) {
        buildFile << """
            plugins {
                id 'elasticsearch.licensing'
            }

            abstract class PrintGitInfo extends DefaultTask {
                @Input
                abstract Property<String> getProjectPath()

                @Input
                abstract Property<String> getRevision()

                @Input
                abstract Property<String> getOrigin()

                @TaskAction
                void print() {
                    println "GITINFO \${projectPath.get()} revision=\${revision.get()} origin=\${origin.get()}"
                }
            }

            def gitInfo = plugins.getPlugin(org.elasticsearch.gradle.internal.conventions.GitInfoPlugin).gitInfo
            // build tree path includes the included build's path, e.g. ':elasticsearch:lib'
            def projectBuildTreePath = buildTreePath
            tasks.register('printGitInfo', PrintGitInfo) {
                projectPath = projectBuildTreePath
                revision = gitInfo.map { it.revision }
                origin = gitInfo.map { it.origin }
            }
        """
        buildFile
    }
}
