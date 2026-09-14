/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ospackage

import org.apache.commons.compress.archivers.ar.ArArchiveInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.elasticsearch.gradle.fixtures.AbstractJavaGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import org.redline_rpm.ReadableChannelWrapper
import org.redline_rpm.Scanner
import org.redline_rpm.header.Header
import spock.lang.IgnoreIf

import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

/**
 * Covers the behavior of the packaging tasks the Elasticsearch distribution build relies on:
 * declarative content mappings with ownership/mode metadata, package-owned parent and empty
 * directory entries, config file handling and preservation of in-tree symbolic links — all
 * exercised under configuration cache compatibility checking (the base fixture runs every build
 * twice with the configuration cache enabled).
 */
// Creates symbolic links in the test workspace, which is unsupported on Windows agents.
@IgnoreIf({ os.isWindows() })
class SystemPackagingTaskFuncTest extends AbstractJavaGradleFuncTest {

    def setup() {
        settingsFile.text = """
        plugins {
            id 'elasticsearch.java-toolchain'
        }

        toolchainManagement {
          jvm {
            javaRepositories {
              repository('bundledOracleOpendJdk') {
                resolverClass = org.elasticsearch.gradle.internal.toolchain.OracleOpenJdkToolchainResolver
              }
              repository('adoptiumJdks') {
                resolverClass = org.elasticsearch.gradle.internal.toolchain.AdoptiumJdkToolchainResolver
              }
              repository('archivedOracleJdks') {
                resolverClass = org.elasticsearch.gradle.internal.toolchain.ArchivedOracleJdkToolchainResolver
              }
            }
          }
        }
        """ + settingsFile.text

        file('files/bin/run.sh') << "#!/bin/bash\necho hello\n"
        file('files/conf/app.conf') << "setting: value\n"
        file('files/conf/sub/nested.conf') << "nested: value\n"
        file('files/lib/real.txt') << "real content\n"
        Path link = file('files/lib/link.txt').toPath()
        Files.deleteIfExists(link)
        Files.createSymbolicLink(link, Path.of('real.txt'))

        buildFile << """
        import org.elasticsearch.gradle.internal.ospackage.deb.Deb
        import org.elasticsearch.gradle.internal.ospackage.rpm.Rpm
        import org.redline_rpm.payload.Directive

        plugins {
          // bring build-tools-internal onto the classpath
          id 'elasticsearch.global-build-info'
        }

        version = '1.2.3-SNAPSHOT+build42'
        def normalizedVersion = project.version.toString().replaceAll('\\\\+.*', '').replace('-', '~')

        def commonConfig = { packageVersion ->
            return {
                packageName = 'test-pkg'
                version = packageVersion
                destinationDirectory = file('build/dists')
                maintainer = 'Test <test@example.org>'
                summary = 'a test package'
                packageDescription = 'longer description'
                url = 'https://example.org'
                user = 'root'
                permissionGroup = 'root'
                requires 'coreutils'
                conflicts 'other-pkg'

                from('files/bin') {
                    into '/opt/test/bin'
                    fileMode 0755
                    ownParentDirectories '/opt/test'
                }
                from('files/lib') {
                    into '/opt/test/lib'
                    fileMode 0644
                    ownParentDirectories '/opt/test'
                }
                from('files/conf') {
                    into '/opt/test/conf'
                    user 'testuser'
                    permissionGroup 'testgroup'
                    setgid true
                    ownDirectories true
                    fileMode 0660
                    dirMode 0750
                    fileType Directive.RPMFILE_CONFIG | Directive.RPMFILE_NOREPLACE
                }
                configurationFile '/opt/test/conf/app.conf'
                directory('/var/log/test-pkg', 0750, 'testuser', 'testgroup', true)
            }
        }

        tasks.register('buildRpm', Rpm) {
            configure(commonConfig('1.2.3'))
            archiveFileName = 'test-pkg-1.2.3.noarch.rpm'
            arch = 'NOARCH'
            packageGroup = 'Application/Test'
            license = 'Test License'
        }

        tasks.register('buildDeb', Deb) {
            configure(commonConfig('1.2.3'))
            archiveFileName = 'test-pkg_1.2.3_all.deb'
            arch = 'all'
            packageGroup = 'test'
            customFields.put('License', 'Test-License')
        }

        tasks.register('buildQualifiedVersionRpm', Rpm) {
            configure(commonConfig(normalizedVersion))
            archiveFileName = "test-pkg-\${project.version}.noarch.rpm"
            arch = 'NOARCH'
            packageGroup = 'Application/Test'
            license = 'Test License'
        }

        tasks.register('buildQualifiedVersionDeb', Deb) {
            configure(commonConfig(normalizedVersion))
            archiveFileName = "test-pkg_\${project.version}_all.deb"
            arch = 'all'
            packageGroup = 'test'
            customFields.put('License', 'Test-License')
        }
        """
    }

    def "builds rpm with packaging metadata, directory entries and symlinks"() {
        when:
        def result = gradleRunner('buildRpm').build()

        then:
        result.task(":buildRpm").outcome == TaskOutcome.SUCCESS
        def rpm = file("build/dists/test-pkg-1.2.3.noarch.rpm")
        rpm.exists()

        def header = readRpmHeader(rpm)
        headerValues(header, 'NAME') == ['test-pkg']
        headerValues(header, 'VERSION') == ['1.2.3']
        headerValues(header, 'LICENSE') == ['Test License']

        def files = rpmFiles(header)
        files['/opt/test/bin/run.sh'].user == 'root'
        (files['/opt/test/bin/run.sh'].mode & 0777) == 0755
        files['/opt/test/conf/app.conf'].user == 'testuser'
        files['/opt/test/conf/app.conf'].group == 'testgroup'
        (files['/opt/test/conf/app.conf'].mode & 0777) == 0660
        // directory entry with setgid bit from the mapping metadata
        (files['/opt/test/conf/sub'].mode & 07777) == 02750
        // package-owned parent directories below /opt/test
        files.containsKey('/opt/test/bin')
        (files['/opt/test/bin'].mode & 0777) == 0755
        // explicitly declared empty directory
        (files['/var/log/test-pkg'].mode & 07777) == 02750
        files['/var/log/test-pkg'].user == 'testuser'
        // in-tree symlink preserved as a link entry
        files['/opt/test/lib/link.txt'].linkTarget == 'real.txt'
    }

    def "builds deb with packaging metadata, directory entries and symlinks"() {
        when:
        def result = gradleRunner('buildDeb').build()

        then:
        result.task(":buildDeb").outcome == TaskOutcome.SUCCESS
        def deb = file("build/dists/test-pkg_1.2.3_all.deb")
        deb.exists()

        def control = readDebControlFile(deb, './control')
        control.contains('Package: test-pkg')
        control.contains('Version: 1.2.3')
        // the XB- prefixed custom field is rendered without the prefix in the binary control file
        control.contains('License: Test-License')
        readDebControlFile(deb, './conffiles').contains('/opt/test/conf/app.conf')
        def postinst = readDebControlFile(deb, './postinst')
        postinst.contains('install -o testuser -g testgroup -m 2750 -d /opt/test/conf/sub')
        postinst.contains('install -o testuser -g testgroup -m 2750 -d /var/log/test-pkg')

        def entries = readDebDataEntries(deb)
        entries['/opt/test/bin/run.sh'].userName == 'root'
        (entries['/opt/test/bin/run.sh'].mode & 0777) == 0755
        entries['/opt/test/conf/app.conf'].userName == 'testuser'
        entries['/opt/test/conf/app.conf'].groupName == 'testgroup'
        (entries['/opt/test/conf/app.conf'].mode & 0777) == 0660
        (entries['/opt/test/conf/sub'].mode & 07777) == 02750
        (entries['/var/log/test-pkg'].mode & 07777) == 02750
        // in-tree symlink preserved as a link entry
        entries['/opt/test/lib/link.txt'].isSymbolicLink()
        entries['/opt/test/lib/link.txt'].linkName == 'real.txt'
    }

    def "normalizes qualified project versions for package metadata"() {
        when:
        def result = gradleRunner('buildQualifiedVersionRpm', 'buildQualifiedVersionDeb').build()

        then:
        result.task(':buildQualifiedVersionRpm').outcome == TaskOutcome.SUCCESS
        result.task(':buildQualifiedVersionDeb').outcome == TaskOutcome.SUCCESS

        def rpm = file('build/dists/test-pkg-1.2.3-SNAPSHOT+build42.noarch.rpm')
        rpm.exists()
        headerValues(readRpmHeader(rpm), 'VERSION') == ['1.2.3~SNAPSHOT']

        def deb = file('build/dists/test-pkg_1.2.3-SNAPSHOT+build42_all.deb')
        deb.exists()
        def control = readDebControlFile(deb, './control')
        control.contains('Version: 1.2.3~SNAPSHOT')
        control.contains('Package: test-pkg')
        control.contains('build42') == false
    }

    private static Header readRpmHeader(File rpm) {
        rpm.withInputStream { input ->
            return new Scanner().run(new ReadableChannelWrapper(Channels.newChannel(input))).header
        }
    }

    private static List headerValues(Header header, String tagName) {
        def entry = header.getEntry(Header.HeaderTag.valueOf(tagName))
        entry == null ? [] : (entry.values as List)
    }

    private static Map<String, Map> rpmFiles(Header header) {
        List dirNames = headerValues(header, 'DIRNAMES')
        List dirIndexes = headerValues(header, 'DIRINDEXES')
        List baseNames = headerValues(header, 'BASENAMES')
        List modes = headerValues(header, 'FILEMODES')
        List users = headerValues(header, 'FILEUSERNAME')
        List groups = headerValues(header, 'FILEGROUPNAME')
        List linkTargets = headerValues(header, 'FILELINKTOS')
        Map<String, Map> result = [:]
        for (int i = 0; i < baseNames.size(); i++) {
            String path = "${dirNames[dirIndexes[i] as int]}${baseNames[i]}"
            result[path] = [
                mode: (modes[i] as int) & 07777,
                user: users[i],
                group: groups[i],
                linkTarget: linkTargets ? (linkTargets[i] ?: '') : ''
            ]
        }
        return result
    }

    private static String readDebControlFile(File deb, String name) {
        String content = null
        eachDebTarEntry(deb, "control.tar.gz") { TarArchiveEntry entry, TarArchiveInputStream tar ->
            if (entry.name == name) {
                content = new String(tar.readAllBytes(), StandardCharsets.UTF_8)
            }
        }
        return content
    }

    private static Map<String, TarArchiveEntry> readDebDataEntries(File deb) {
        Map<String, TarArchiveEntry> entries = [:]
        eachDebTarEntry(deb, "data.tar.gz") { TarArchiveEntry entry, TarArchiveInputStream tar ->
            String name = entry.name.replaceFirst(/^\./, '')
            if (name.length() > 1 && name.endsWith('/')) {
                name = name.substring(0, name.length() - 1)
            }
            entries[name] = entry
        }
        return entries
    }

    private static void eachDebTarEntry(File deb, String archiveName, Closure callback) {
        deb.withInputStream { input ->
            def ar = new ArArchiveInputStream(input)
            def arEntry
            while ((arEntry = ar.nextEntry) != null) {
                if (arEntry.name == archiveName) {
                    def tar = new TarArchiveInputStream(new GzipCompressorInputStream(ar))
                    def tarEntry
                    while ((tarEntry = tar.nextEntry) != null) {
                        callback(tarEntry, tar)
                    }
                }
            }
        }
    }
}
