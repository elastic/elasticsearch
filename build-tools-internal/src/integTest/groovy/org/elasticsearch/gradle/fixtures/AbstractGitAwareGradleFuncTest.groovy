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
import org.gradle.util.GradleVersion

import java.io.FileFilter
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.UUID

abstract class AbstractGitAwareGradleFuncTest extends AbstractGradleInternalPluginFuncTest {

    private static final String WRAPPER_DISTS_RELATIVE_PATH = "wrapper/dists"

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
        seedTestKitWrapperCache()
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

    private static void seedTestKitWrapperCache() {
        String testKitDirPath = System.getProperty("org.gradle.testkit.dir")
        if (testKitDirPath == null) {
            return
        }
        String currentWrapperDistributionDirName = "gradle-${GradleVersion.current().version}-bin"
        File testKitWrapperDistributionDir = new File(testKitDirPath, WRAPPER_DISTS_RELATIVE_PATH + "/" + currentWrapperDistributionDirName)
        if (isReadyWrapperDistribution(testKitWrapperDistributionDir)) {
            return
        }
        File gradleUserHome = resolveGradleUserHome()
        File localWrapperDistributionDir = new File(gradleUserHome, "${WRAPPER_DISTS_RELATIVE_PATH}/${currentWrapperDistributionDirName}")
        if (isReadyWrapperDistribution(localWrapperDistributionDir) == false) {
            return
        }
        File wrapperSeedLock = new File(testKitWrapperDistributionDir.parentFile, currentWrapperDistributionDirName + ".seed.lock")
        withExclusiveFileLock(wrapperSeedLock) {
            if (isReadyWrapperDistribution(testKitWrapperDistributionDir)) {
                return
            }
            if (testKitWrapperDistributionDir.exists()) {
                FileUtils.deleteDirectory(testKitWrapperDistributionDir)
            }
            File stagingDir = new File(testKitWrapperDistributionDir.parentFile, testKitWrapperDistributionDir.name + ".tmp-" + UUID.randomUUID())
            FileUtils.deleteQuietly(stagingDir)
            FileUtils.copyDirectory(localWrapperDistributionDir, stagingDir)
            Files.move(stagingDir.toPath(), testKitWrapperDistributionDir.toPath(), StandardCopyOption.ATOMIC_MOVE)
        }
    }

    private static boolean isReadyWrapperDistribution(File wrapperDistributionDir) {
        if (wrapperDistributionDir.isDirectory() == false) {
            return false
        }
        String extractedGradleDirName = wrapperDistributionDir.name.endsWith("-bin")
            ? wrapperDistributionDir.name.substring(0, wrapperDistributionDir.name.length() - "-bin".length())
            : wrapperDistributionDir.name
        File[] hashDirs = wrapperDistributionDir.listFiles({ File file -> file.isDirectory() } as FileFilter)
        if (hashDirs == null || hashDirs.length == 0) {
            return false
        }
        return hashDirs.any { hashDir ->
            new File(hashDir, wrapperDistributionDir.name + ".zip.ok").isFile()
                && new File(hashDir, extractedGradleDirName + "/bin/gradle").isFile()
        }
    }

    private static void withExclusiveFileLock(File lockFile, Closure<?> action) {
        lockFile.parentFile.mkdirs()
        try (FileChannel channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.lock().withCloseable {
                action.call()
            }
        }
    }

    private static File resolveGradleUserHome() {
        String explicitGradleUserHome = System.getProperty("gradle.user.home")
        if (explicitGradleUserHome != null) {
            return new File(explicitGradleUserHome)
        }
        String envGradleUserHome = System.getenv("GRADLE_USER_HOME")
        if (envGradleUserHome != null) {
            return new File(envGradleUserHome)
        }
        return new File(System.getProperty("user.home"), ".gradle")
    }

    GradleRunner gradleRunner(String... arguments) {
        gradleRunner(new File(testProjectDir.root, "cloned"), arguments)
    }
}
