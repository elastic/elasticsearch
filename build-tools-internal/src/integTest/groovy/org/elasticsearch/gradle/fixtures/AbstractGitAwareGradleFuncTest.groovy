/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.fixtures

import spock.lang.Shared
import spock.lang.TempDir

import org.apache.commons.io.FileUtils
import org.gradle.testkit.runner.GradleRunner

abstract class AbstractGitAwareGradleFuncTest extends AbstractGradleFuncTest {

    /**
     * Shared temporary directory for the prepared git remote. Using {@code @Shared @TempDir}
     * ensures the directory is created once per spec class and cleaned up after all methods
     * have run. The remote repo is prepared lazily on first access and reused across methods.
     */
    @Shared
    @TempDir
    File sharedRemoteRepoDir

    @Shared
    File preparedRemoteGitDir

    File remoteGitRepo

    def setup() {
        if (preparedRemoteGitDir == null) {
            preparedRemoteGitDir = setupGitRemote()
        }
        remoteGitRepo = new File(preparedRemoteGitDir, '.git')
        execute("git clone ${remoteGitRepo.absolutePath} cloned", testProjectDir.root)
        buildFile = new File(testProjectDir.root, 'cloned/build.gradle')
        settingsFile = new File(testProjectDir.root, 'cloned/settings.gradle')
        versionPropertiesFile = new File(testProjectDir.root, 'cloned/build-tools-internal/version.properties')
        versionPropertiesFile.text = """
            elasticsearch     = 9.1.0
            lucene            = 10.2.2

            bundled_jdk_vendor = openjdk
            bundled_jdk = 24+36@1f9ff9062db4449d8ca828c504ffae90
            minimumJdkVersion = 21
            minimumRuntimeJava = 21
            minimumCompilerJava = 21
        """
    }

    File setupGitRemote() {
        URL fakeRemote = getClass().getResource("fake_git/remote")
        File workingRemoteGit = new File(sharedRemoteRepoDir, 'remote')
        FileUtils.copyDirectory(new File(fakeRemote.toURI()), workingRemoteGit)
        fakeRemote.file + "/.git"
        gradleRunner(workingRemoteGit, "wrapper").build()

        execute("git init", workingRemoteGit)
        execute('git config user.email "build-tool@elastic.co"', workingRemoteGit)
        execute('git config user.name "Build tool"', workingRemoteGit)
        execute("git add .", workingRemoteGit)
        execute('git commit -m"Initial"', workingRemoteGit)
        return workingRemoteGit;
    }

    GradleRunner gradleRunner(String... arguments) {
        gradleRunner(new File(testProjectDir.root, "cloned"), arguments)
    }
}
