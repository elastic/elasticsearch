/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.tools.zip.ZipEntry
import org.apache.tools.zip.ZipFile
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.IgnoreIf

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions

@IgnoreIf({ os.isWindows() })
class DistributionLayoutPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = DistributionLayoutPlugin

    def setup() {
        setupFixtureFiles()

        buildFile << """
            import org.apache.tools.ant.filters.ReplaceTokens
            import org.gradle.api.tasks.bundling.Compression

            apply plugin: 'base'

            version = project.findProperty('distVersion') ?: '9.1.0'

            esDistributionLayout {
              sourceDirectory.set(layout.projectDirectory.dir('src'))
              defaultModules.from(fileTree('fixtures/modules-default'))
              integTestModules.from(fileTree('fixtures/modules-integ'))
              externalTestModules.from(fileTree('fixtures/modules-external'))
              defaultConfig.from(fileTree('fixtures/config-default'))
              integTestConfig.from(fileTree('fixtures/config-integ'))
              log4jConfig.from(file('fixtures/generated/log4j2.properties'))
              defaultBin.from(fileTree('fixtures/bin-default'))
              integTestBin.from(fileTree('fixtures/bin-integ'))
              defaultNotice.from(file('fixtures/notice/NOTICE.txt'))
              integTestNotice.from(file('fixtures/notice/NOTICE-SERVER.txt'))
            }

            dependencies {
              libs files('fixtures/libs/server.jar')
              libsVersionChecker files('fixtures/libs/java-version-checker.jar')
              libsCliLauncher files('fixtures/libs/cli-launcher.jar')
              libsServerCli files('fixtures/libs/server-cli.jar')
              libsServerLauncher files('fixtures/libs/server-launcher.jar')
              libsServerLauncherNativeX64 files('fixtures/libs/native-launchers/x64/server-launcher')
              libsServerLauncherNativeAarch64 files('fixtures/libs/native-launchers/aarch64/server-launcher')
              libsWindowsServiceCli files('fixtures/libs/windows-service-cli.jar')
              libsPluginCli files('fixtures/libs/plugin-cli.jar')
              libsKeystoreCli files('fixtures/libs/keystore-cli.jar')
              libsSecurityCli files('fixtures/libs/security-cli.jar')
              libsGeoIpCli files('fixtures/libs/geoip-cli.jar')
              libsAnsiConsole files('fixtures/libs/ansi-console.jar')
              libsNative files('fixtures/libs/platform')
              libsEntitlementAgent files('fixtures/libs/entitlement-agent.jar')
              libsEntitlementBridge files('fixtures/libs/entitlement-bridge.jar')
            }

            tasks.register('buildLinuxTar', Tar) {
              archiveFileName = "linux-x64.tar.gz"
              compression = Compression.GZIP
              destinationDirectory = layout.buildDirectory.dir('distributions')
              into("elasticsearch-") {
                into('lib') { with esDistributionLayout.lib('linux', 'x64') }
                into('config') { with esDistributionLayout.config('tar', false) }
                into('bin') { with esDistributionLayout.bin('tar', false) }
                into('modules') { with esDistributionLayout.modules('linux', 'x64') }
                with esDistributionLayout.notice(false)
              }
            }

            tasks.register('buildIntegTestZip', Zip) {
              archiveFileName = "integ-test.zip"
              destinationDirectory = layout.buildDirectory.dir('distributions')
              into("elasticsearch-") {
                into('lib') { with esDistributionLayout.lib(null, null) }
                into('config') { with esDistributionLayout.config('zip', true) }
                into('bin') { with esDistributionLayout.bin('zip', true) }
                into('modules') { with esDistributionLayout.integTestModules() }
                with esDistributionLayout.notice(true)
              }
            }

            tasks.register('buildDebCopy', Copy) {
              into(layout.buildDirectory.dir('deb-copy'))

              with copySpec {
                from('fixtures/packaging-template')
                filter('tokens': esDistributionLayout.tokenExpansions('deb', false), ReplaceTokens.class)
              }

              into('usr/share/elasticsearch/lib') {
                with esDistributionLayout.lib('linux', 'x64')
              }
              into('usr/share/elasticsearch/modules') {
                with esDistributionLayout.modules('linux', 'x64')
              }
              into('usr/share/elasticsearch/bin') {
                with esDistributionLayout.bin('deb', false)
              }
              into('etc/elasticsearch') {
                with esDistributionLayout.config('deb', false)
              }
              into('usr/share/elasticsearch') {
                with esDistributionLayout.notice(false)
              }
            }
        """
    }

    def "builds layout specs and reuses configuration cache"() {
        when:
        BuildResult first = gradleRunner('buildLinuxTar', 'buildIntegTestZip', 'buildDebCopy').build()

        then:
        first.task(':buildLinuxTar').outcome == TaskOutcome.SUCCESS
        first.task(':buildIntegTestZip').outcome == TaskOutcome.SUCCESS
        first.task(':buildDebCopy').outcome == TaskOutcome.SUCCESS

        and:
        File tarFile = file('build/distributions/linux-x64.tar.gz')
        File zipFile = file('build/distributions/integ-test.zip')
        File debCopyDir = file('build/deb-copy')

        List<String> tarListing = tarListing(tarFile)
        List<String> zipListing = zipListing(zipFile)
        List<String> dirListing = directoryListing(debCopyDir.toPath())

        tarListing == golden('layout/linux-tar.txt')
        zipListing == golden('layout/integ-test-zip.txt')
        dirListing == golden('layout/deb-copy.txt')

        and:
        String linuxScript = tarContent(tarFile, 'elasticsearch-/bin/elasticsearch')
        linuxScript.contains('# Built 9.1.0')
        linuxScript.contains('@project.version@') == false

        String windowsBat = zipContent(zipFile, 'elasticsearch-/bin/elasticsearch.bat')
        windowsBat.contains('@project.version@') == false
        windowsBat.contains('\r\n')

        // Keep this fixture single-variant for runtime; verify the linux x64 archive picks the x64 launcher content.
        String linuxServerLauncher = tarContent(tarFile, 'elasticsearch-/lib/tools/server-launcher/server-launcher')
        linuxServerLauncher == 'linux x64 launcher\n'

        String debTemplate = Files.readString(debCopyDir.toPath().resolve('distribution.env'))
        debTemplate.contains('/etc/elasticsearch')
        debTemplate.contains('@path.conf@') == false

        and:
        tarListing.any { it.contains('modules/sample/platform/linux-x86_64/native.dat') }
        tarListing.any { it.contains('modules/sample/platform/darwin-aarch64/native.dat') } == false
        tarListing.any { it.contains('modules/geo/') } == false

        when:
        BuildResult second = gradleRunner('buildLinuxTar', 'buildIntegTestZip', 'buildDebCopy').build()

        then:
        second.output.contains('Configuration cache entry reused.')
        second.task(':buildLinuxTar').outcome == TaskOutcome.UP_TO_DATE
        second.task(':buildIntegTestZip').outcome == TaskOutcome.UP_TO_DATE
        second.task(':buildDebCopy').outcome == TaskOutcome.UP_TO_DATE

        when:
        BuildResult third = gradleRunner('buildLinuxTar', 'buildIntegTestZip', 'buildDebCopy', '-PdistVersion=9.1.1').build()

        then:
        third.task(':buildLinuxTar').outcome == TaskOutcome.SUCCESS
        third.task(':buildIntegTestZip').outcome == TaskOutcome.SUCCESS
        third.task(':buildDebCopy').outcome == TaskOutcome.SUCCESS

        and:
        String updatedLinuxScript = tarContent(file('build/distributions/linux-x64.tar.gz'), 'elasticsearch-/bin/elasticsearch')
        updatedLinuxScript.contains('# Built 9.1.1')
        updatedLinuxScript.contains('# Built 9.1.0') == false

        String updatedDebScript = Files.readString(file('build/deb-copy/usr/share/elasticsearch/bin/elasticsearch').toPath())
        updatedDebScript.contains('# Built 9.1.1')

        String updatedWindowsBat = zipContent(file('build/distributions/integ-test.zip'), 'elasticsearch-/bin/elasticsearch.bat')
        updatedWindowsBat.contains('set VERSION=9.1.1')
    }

    private void setupFixtureFiles() {
        file('licenses/ELASTIC-LICENSE-2.0.txt').text = 'Elastic License 2.0\n'
        file('licenses/AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt').text = 'Test Distribution License\n'

        file('src/config/elasticsearch.yml').text = 'name: @project.name@\npath.logs: @path.logs@\n'
        file('src/config/elasticsearch-plugins.example.yml').text = 'plugins: true\n'
        file('src/bin/elasticsearch').text = '#!/bin/sh\n@source.path.env@\n# Built @project.version@\n'
        file('src/bin/elasticsearch.bat').text = '@echo off\nset VERSION=@project.version@\n'
        file('src/bin/elasticsearch.exe').bytes = [1, 2, 3] as byte[]

        file('fixtures/generated/log4j2.properties').text = 'rootLogger.level = info\n'
        file('fixtures/config-default/default.yml').text = 'default: true\n'
        file('fixtures/config-integ/integ.yml').text = 'integ: true\n'
        file('fixtures/bin-default/module-tool').text = '#!/bin/sh\nmodule\n'
        file('fixtures/bin-default/module-tool.bat').text = '@echo off\nmodule\n'
        file('fixtures/bin-integ/integ-tool').text = '#!/bin/sh\ninteg\n'

        file('fixtures/notice/NOTICE.txt').text = 'Elasticsearch\nCopyright 2009-2024 Elasticsearch\n'
        file('fixtures/notice/NOTICE-SERVER.txt').text = 'Server Notice\n'

        file('fixtures/modules-default/sample/bin/sample-tool').text = '#!/bin/sh\nsample\n'
        file('fixtures/modules-default/sample/config/sample.yml').text = 'sample: true\n'
        file('fixtures/modules-default/sample/platform/linux-x86_64/native.dat').text = 'linux\n'
        file('fixtures/modules-default/sample/platform/darwin-aarch64/native.dat').text = 'darwin\n'
        file('fixtures/modules-default/geo/ignored/file.txt').text = 'geo\n'

        file('fixtures/modules-integ/transport/bin/integ-tool').text = '#!/bin/sh\ntransport\n'
        file('fixtures/modules-external/external/module.txt').text = 'external\n'

        file('fixtures/packaging-template/distribution.env').text = 'CONF=@path.conf@\nENV=@path.env@\n'

        file('fixtures/libs/server.jar').text = 'server\n'
        file('fixtures/libs/java-version-checker.jar').text = 'version checker\n'
        file('fixtures/libs/cli-launcher.jar').text = 'cli launcher\n'
        file('fixtures/libs/server-cli.jar').text = 'server cli\n'
        file('fixtures/libs/server-launcher.jar').text = 'server launcher\n'
        file('fixtures/libs/native-launchers/x64/server-launcher').text = 'linux x64 launcher\n'
        file('fixtures/libs/native-launchers/aarch64/server-launcher').text = 'linux aarch64 launcher\n'
        file('fixtures/libs/windows-service-cli.jar').text = 'windows service\n'
        file('fixtures/libs/plugin-cli.jar').text = 'plugin cli\n'
        file('fixtures/libs/keystore-cli.jar').text = 'keystore cli\n'
        file('fixtures/libs/security-cli.jar').text = 'security cli\n'
        file('fixtures/libs/geoip-cli.jar').text = 'geoip cli\n'
        file('fixtures/libs/ansi-console.jar').text = 'ansi\n'
        file('fixtures/libs/entitlement-agent.jar').text = 'agent\n'
        entitlementBridgeJar(file('fixtures/libs/entitlement-bridge.jar'))

        file('fixtures/libs/platform/linux-x64/libnative.so').text = 'native linux\n'
        file('fixtures/libs/platform/darwin-aarch64/libnative.dylib').text = 'native darwin\n'

        // Keep fixture source modes deterministic across different host umasks.
        List<String> readOnlyFiles = [
            'licenses/ELASTIC-LICENSE-2.0.txt',
            'licenses/AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt',
            'src/config/elasticsearch.yml',
            'src/config/elasticsearch-plugins.example.yml',
            'src/bin/elasticsearch.bat',
            'src/bin/elasticsearch.exe',
            'fixtures/generated/log4j2.properties',
            'fixtures/config-default/default.yml',
            'fixtures/config-integ/integ.yml',
            'fixtures/bin-default/module-tool.bat',
            'fixtures/notice/NOTICE.txt',
            'fixtures/notice/NOTICE-SERVER.txt',
            'fixtures/modules-default/sample/config/sample.yml',
            'fixtures/modules-default/sample/platform/linux-x86_64/native.dat',
            'fixtures/modules-default/sample/platform/darwin-aarch64/native.dat',
            'fixtures/modules-default/geo/ignored/file.txt',
            'fixtures/modules-integ/transport/bin/integ-tool',
            'fixtures/modules-external/external/module.txt',
            'fixtures/packaging-template/distribution.env',
            'fixtures/libs/server.jar',
            'fixtures/libs/java-version-checker.jar',
            'fixtures/libs/cli-launcher.jar',
            'fixtures/libs/server-cli.jar',
            'fixtures/libs/server-launcher.jar',
            'fixtures/libs/native-launchers/x64/server-launcher',
            'fixtures/libs/native-launchers/aarch64/server-launcher',
            'fixtures/libs/windows-service-cli.jar',
            'fixtures/libs/plugin-cli.jar',
            'fixtures/libs/keystore-cli.jar',
            'fixtures/libs/security-cli.jar',
            'fixtures/libs/geoip-cli.jar',
            'fixtures/libs/ansi-console.jar',
            'fixtures/libs/entitlement-agent.jar',
            'fixtures/libs/entitlement-bridge.jar',
            'fixtures/libs/platform/linux-x64/libnative.so',
            'fixtures/libs/platform/darwin-aarch64/libnative.dylib'
        ]
        readOnlyFiles.forEach { setFixtureMode(it, 'rw-r--r--') }

        List<String> executableFiles = [
            'src/bin/elasticsearch',
            'fixtures/bin-default/module-tool',
            'fixtures/bin-integ/integ-tool',
            'fixtures/modules-default/sample/bin/sample-tool'
        ]
        executableFiles.forEach { setFixtureMode(it, 'rwxr-xr-x') }
    }

    private void setFixtureMode(String path, String permissions) {
        Files.setPosixFilePermissions(file(path).toPath(), PosixFilePermissions.fromString(permissions))
    }

    private static List<String> tarListing(File tarArchive) {
        List<String> entries = []
        TarArchiveInputStream tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(tarArchive)))
        try {
            def current = tarInput.nextTarEntry
            while (current != null) {
                entries.add("${current.name} ${String.format('%04o', current.mode)}")
                current = tarInput.nextTarEntry
            }
        } finally {
            tarInput.close()
        }
        entries.sort()
    }

    private static List<String> zipListing(File archive) {
        List<String> entries = []
        ZipFile zip = new ZipFile(archive)
        try {
            Enumeration<ZipEntry> all = zip.entries
            while (all.hasMoreElements()) {
                ZipEntry entry = all.nextElement()
                entries.add("${entry.name} ${String.format('%04o', entry.unixMode)}")
            }
        } finally {
            zip.close()
        }
        entries.sort()
    }

    private static List<String> directoryListing(Path root) {
        List<String> entries = []
        Files.walk(root).forEach { path ->
            if (path != root) {
                String relative = root.relativize(path).toString().replace('\\\\', '/')
                int mode = mode(path)
                entries.add("${relative} ${String.format('%04o', mode)}")
            }
        }
        entries.sort()
    }

    private static int mode(Path path) {
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path)
        int mode = 0
        if (permissions.contains(PosixFilePermission.OWNER_READ)) mode |= 0400
        if (permissions.contains(PosixFilePermission.OWNER_WRITE)) mode |= 0200
        if (permissions.contains(PosixFilePermission.OWNER_EXECUTE)) mode |= 0100
        if (permissions.contains(PosixFilePermission.GROUP_READ)) mode |= 0040
        if (permissions.contains(PosixFilePermission.GROUP_WRITE)) mode |= 0020
        if (permissions.contains(PosixFilePermission.GROUP_EXECUTE)) mode |= 0010
        if (permissions.contains(PosixFilePermission.OTHERS_READ)) mode |= 0004
        if (permissions.contains(PosixFilePermission.OTHERS_WRITE)) mode |= 0002
        if (permissions.contains(PosixFilePermission.OTHERS_EXECUTE)) mode |= 0001
        return mode
    }

    private static String tarContent(File tarArchive, String path) {
        TarArchiveInputStream tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(tarArchive)))
        try {
            def current = tarInput.nextTarEntry
            while (current != null) {
                if (current.name == path) {
                    return new String(tarInput.readAllBytes(), StandardCharsets.UTF_8)
                }
                current = tarInput.nextTarEntry
            }
            throw new AssertionError("Missing tar entry " + path)
        } finally {
            tarInput.close()
        }
    }

    private static String zipContent(File archive, String path) {
        ZipFile zip = new ZipFile(archive)
        try {
            ZipEntry entry = zip.getEntry(path)
            assert entry != null : "Missing zip entry " + path
            return new String(zip.getInputStream(entry).readAllBytes(), StandardCharsets.UTF_8)
        } finally {
            zip.close()
        }
    }

    private List<String> golden(String name) {
        String text = DistributionLayoutPluginFuncTest.class.getResource("/${DistributionLayoutPluginFuncTest.package.name.replace('.', '/')}/" + name).text
        text.readLines().findAll { it.isEmpty() == false }
    }

    private static void entitlementBridgeJar(File file) {
        file.parentFile.mkdirs()
        def jar = new java.util.jar.JarOutputStream(new FileOutputStream(file))
        try {
            jar.putNextEntry(new java.util.jar.JarEntry('bridge.class'))
            jar.write([1, 2] as byte[])
            jar.closeEntry()

            jar.putNextEntry(new java.util.jar.JarEntry('module-info.class'))
            jar.write([3, 4] as byte[])
            jar.closeEntry()

            jar.putNextEntry(new java.util.jar.JarEntry('META-INF/versions/9/bridge.class'))
            jar.write([5, 6] as byte[])
            jar.closeEntry()
        } finally {
            jar.close()
        }
    }
}
