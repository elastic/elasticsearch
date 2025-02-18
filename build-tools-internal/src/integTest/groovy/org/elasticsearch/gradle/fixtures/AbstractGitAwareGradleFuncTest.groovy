/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.fixtures

import org.apache.commons.io.FileUtils
import org.elasticsearch.gradle.internal.test.InternalAwareGradleRunner
import org.gradle.testkit.runner.GradleRunner
import org.junit.Rule
import org.junit.rules.TemporaryFolder

abstract class AbstractGitAwareGradleFuncTest extends AbstractGradleFuncTest {

    @Rule
    TemporaryFolder remoteRepoDirs = new TemporaryFolder()

    File remoteGitRepo

    def setup() {
        remoteGitRepo = new File(setupGitRemote(), '.git')
        execute("git clone ${remoteGitRepo.absolutePath} cloned", testProjectDir.root)
        buildFile = new File(testProjectDir.root, 'cloned/build.gradle')
        settingsFile = new File(testProjectDir.root, 'cloned/settings.gradle')
    }

    File setupGitRemote() {
        URL fakeRemote = getClass().getResource("fake_git/remote")
        File workingRemoteGit = new File(remoteRepoDirs.root, 'remote')
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
