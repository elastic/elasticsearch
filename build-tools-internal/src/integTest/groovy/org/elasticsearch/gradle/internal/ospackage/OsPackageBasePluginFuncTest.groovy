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
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome
import org.redline_rpm.ReadableChannelWrapper
import org.redline_rpm.Scanner
import org.redline_rpm.header.Header

import java.nio.channels.Channels

/**
 * Covers the parts of the extracted ospackage plugin the Elasticsearch distribution build relies
 * on: project wide defaults via the {@code ospackage} extension, per copy-spec packaging
 * attributes (owner, group, setgid, directory entries) and registering additional directory
 * entries from {@code eachFile} callbacks at execution time — all under the configuration cache
 * (the base test fixture runs every build with configuration cache compatibility checking).
 */
class OsPackageBasePluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = OsPackageBasePlugin.class

    def setup() {
        file('files/bin/run.sh') << "#!/bin/bash\necho hello\n"
        file('files/conf/app.conf') << "setting: value\n"
        file('files/conf/sub/nested.conf') << "nested: value\n"
        buildFile << """
        import org.elasticsearch.gradle.internal.ospackage.deb.Deb
        import org.elasticsearch.gradle.internal.ospackage.rpm.Rpm

        ospackage {
            maintainer = 'Test <test@example.org>'
            summary = 'a test package'
            packageDescription = 'longer description'
            url = 'https://example.org'
            user = 'root'
            permissionGroup = 'root'
            fileMode = 0644
            dirMode = 0755
        }

        def commonConfig = {
            def packagingTask = delegate
            packageName = 'test-pkg'
            version = '1.2.3'
            arch = 'NOARCH'
            destinationDirectory = file('build/dists')
            into('/opt/test') {
                into('bin') {
                    from('files/bin') {
                        filePermissions { unix(0755) }
                    }
                    eachFile { details ->
                        // registering directory entries while the task is executing must work
                        packagingTask.directory('/opt/test/registered', 0750)
                    }
                }
                into('conf') {
                    from('files/conf') {
                        user 'testuser'
                        permissionGroup 'testgroup'
                        setgid true
                        createDirectoryEntry true
                        filePermissions { unix(0660) }
                        dirPermissions { unix(0750) }
                    }
                }
            }
            configurationFile '/opt/test/app.conf'
            requires 'coreutils'
            conflicts 'other-pkg'
        }

        tasks.register('buildRpm', Rpm) {
            configure(commonConfig)
            archiveFileName = 'test-pkg-1.2.3.noarch.rpm'
            packageGroup = 'Application/Test'
            license = 'Test License'
        }

        tasks.register('buildDeb', Deb) {
            configure(commonConfig)
            archiveFileName = 'test-pkg_1.2.3_all.deb'
            arch = 'all'
            packageGroup = 'test'
            customFields.put('License', 'Test-License')
        }
        """
    }

    def "builds rpm with packaging attributes and execution time directory entries"() {
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
        // directory entry with setgid bit from the copy spec attributes
        (files['/opt/test/conf/sub'].mode & 07777) == 02750
        // directory entry registered from the eachFile callback at execution time
        files.containsKey('/opt/test/registered')
        (files['/opt/test/registered'].mode & 0777) == 0750
    }

    def "builds deb with packaging attributes and custom control fields"() {
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
        readDebControlFile(deb, './conffiles').contains('/opt/test/app.conf')

        def entries = readDebDataEntries(deb)
        entries['/opt/test/bin/run.sh'].userName == 'root'
        (entries['/opt/test/bin/run.sh'].mode & 0777) == 0755
        entries['/opt/test/conf/app.conf'].userName == 'testuser'
        entries['/opt/test/conf/app.conf'].groupName == 'testgroup'
        (entries['/opt/test/conf/app.conf'].mode & 0777) == 0660
        (entries['/opt/test/conf/sub'].mode & 07777) == 02750
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
        Map<String, Map> result = [:]
        for (int i = 0; i < baseNames.size(); i++) {
            String path = "${dirNames[dirIndexes[i] as int]}${baseNames[i]}"
            result[path] = [mode: (modes[i] as int) & 07777, user: users[i], group: groups[i]]
        }
        return result
    }

    private static String readDebControlFile(File deb, String name) {
        String content = null
        eachDebTarEntry(deb, "control.tar.gz") { TarArchiveEntry entry, TarArchiveInputStream tar ->
            if (entry.name == name) {
                content = new String(tar.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
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
