/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.IgnoreIf
import spock.lang.Unroll

/**
 * Runs the package checks against a stand-in package. {@code dpkg-deb} and {@code rpm} are replaced by shell scripts
 * on the plugin's tool search path, so the tests neither depend on the host having these tools nor need a real
 * package: the scripts "extract" a prepared directory and report a configurable license as package metadata. The
 * command lines passed to the real tools are covered by unit tests.
 */
@IgnoreIf({ os.isWindows() })
class InternalDistributionPackageCheckPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = InternalDistributionPackageCheckPlugin.class

    def setup() {
        file("licenses/ELASTIC-LICENSE-2.0.txt").text = "Elastic License 2.0\nlicense text line 2\n"
        file("SomeFile.txt").text = "content"
        buildFile << """
            tasks.register("buildPackage", Zip) {
                archiveFileName = "elasticsearch.pkg"
                destinationDirectory = layout.buildDirectory.dir("distributions")
                from 'SomeFile.txt'
            }
            distributionPackageCheck {
                packageFile = tasks.named("buildPackage").flatMap { it.archiveFile }
                toolSearchPath = ['${file("tools").absolutePath}']
            }
        """
    }

    @Unroll
    def "checks #type package and reuses the configuration cache"() {
        given:
        packageType(type)
        packageTool(type, expectedMetadataLicense)
        packageContent(type, expectedLicense, "Elasticsearch\nCopyright 2009-2024 Elasticsearch\n")

        when:
        def result = gradleRunner("check").build()

        then:
        result.task(":buildPackage").outcome == TaskOutcome.SUCCESS
        ["checkExtraction", "checkLicense", "checkNotice", "checkLicenseMetadata"].each {
            assert result.task(":$it").outcome == TaskOutcome.SUCCESS
        }
        file("build/extracted/${type}-extracted/usr/share/elasticsearch/NOTICE.txt").exists()
        file("tools/invocations.log").text.contains(file("build/distributions/elasticsearch.pkg").absolutePath)

        when:
        result = gradleRunner("check").build()

        then:
        result.output.contains("Configuration cache entry reused.")
        result.task(":checkExtraction").outcome == TaskOutcome.UP_TO_DATE
        result.task(":checkLicense").outcome == TaskOutcome.SUCCESS
        result.task(":checkLicenseMetadata").outcome == TaskOutcome.SUCCESS

        where:
        type  | expectedLicense                                                         | expectedMetadataLicense
        "deb" | DEB_COPYRIGHT_HEADER + " Elastic License 2.0\n license text line 2\n" | "Elastic-License"
        "rpm" | "Elastic License 2.0\nlicense text line 2\n"                            | "Elastic License"
    }

    @Unroll
    def "fails on unexpected #type license file content"() {
        given:
        packageType(type)
        packageTool(type, "irrelevant")
        packageContent(type, licenseContent, "Elasticsearch\nCopyright 2009-2024 Elasticsearch\n")

        when:
        def result = gradleRunner("checkLicense").buildAndFail()

        then:
        result.task(":checkLicense").outcome == TaskOutcome.FAILED
        assertOutputContains(
            result.output,
            "expected line [$line] in [./build/extracted/${type}-extracted/${licensePath}] to be [$expected] but was [unknown]"
        )

        where:
        type  | licenseContent                                                          | line | licensePath                               | expected
        "deb" | DEB_COPYRIGHT_HEADER + " Elastic License 2.0\nunknown\n"                | 5    | "usr/share/doc/elasticsearch/copyright"   | " license text line 2"
        "rpm" | "unknown\n"                                                             | 1    | "usr/share/elasticsearch/LICENSE.txt"     | "Elastic License 2.0"
    }

    def "fails on unexpected notice file content"() {
        given:
        packageType("rpm")
        packageTool("rpm", "irrelevant")
        packageContent("rpm", "irrelevant", "Elasticsearch\nCopyright 2009-2018 Acme\n")

        when:
        def result = gradleRunner("checkNotice").buildAndFail()

        then:
        result.task(":checkNotice").outcome == TaskOutcome.FAILED
        assertOutputContains(
            result.output,
            "expected line [2] in [./build/extracted/rpm-extracted/usr/share/elasticsearch/NOTICE.txt] " +
                "to be [Copyright 2009-2024 Elasticsearch] but was [Copyright 2009-2018 Acme]"
        )
    }

    @Unroll
    def "fails on unexpected #type license metadata"() {
        given:
        packageType(type)
        packageTool(type, "Apache-2.0")

        when:
        def result = gradleRunner("checkLicenseMetadata").buildAndFail()

        then:
        result.task(":checkLicenseMetadata").outcome == TaskOutcome.FAILED
        result.output.contains(expectedError)

        where:
        type  | expectedError
        "deb" | "expected license [Elastic-License] for package info but found [Apache-2.0]"
        "rpm" | "expected license [Elastic License] for ["
    }

    def "does not build or check the package without the package tool"() {
        given:
        packageType("deb")
        file("tools").mkdirs()

        when:
        def result = gradleRunner("check").build()

        then:
        result.task(":buildPackage") == null
        result.task(":checkExtraction") == null

        when:
        result = gradleRunner("checkLicense").build()

        then:
        result.task(":checkExtraction").outcome == TaskOutcome.SKIPPED
        result.task(":checkLicense").outcome == TaskOutcome.SKIPPED
    }

    private static final String DEB_COPYRIGHT_HEADER = """\
Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/
Copyright: Elasticsearch B.V. <info@elastic.co>
License: Elastic-License
"""

    private void packageType(String type) {
        buildFile << """
            distributionPackageCheck.packageType = org.elasticsearch.gradle.internal.distribution.PackageMetadata.Type.${type.toUpperCase(Locale.ROOT)}
        """
    }

    /**
     * Lays out the files the stand-in package tool "extracts".
     */
    private void packageContent(String type, String license, String notice) {
        String licensePath = type == "deb" ? "usr/share/doc/elasticsearch/copyright" : "usr/share/elasticsearch/LICENSE.txt"
        file("package-content/$licensePath").text = license
        file("package-content/usr/share/elasticsearch/NOTICE.txt").text = notice
    }

    /**
     * Writes a stand-in for {@code dpkg-deb} or {@code rpm} that records its arguments, copies {@code package-content}
     * into the extraction directory and reports {@code metadataLicense} as the package's license.
     */
    private void packageTool(String type, String metadataLicense) {
        String content = file("package-content").absolutePath
        String log = file("tools/invocations.log").absolutePath
        File tool
        if (type == "deb") {
            tool = file("tools/dpkg-deb")
            tool.text = """#!/bin/sh
echo "\$@" >> '$log'
case "\$1" in
  -x) mkdir -p "\$3" && cp -R '$content/.' "\$3" ;;
  --info) printf ' Package: elasticsearch\\n License: %s\\n' '$metadataLicense' ;;
  *) exit 1 ;;
esac
"""
        } else {
            tool = file("tools/rpm")
            tool.text = """#!/bin/sh
echo "\$@" >> '$log'
if [ "\$1" = "-qp" ]; then
  printf '%s' '$metadataLicense'
  exit 0
fi
dest=""
while [ \$# -gt 0 ]; do
  if [ "\$1" = "--relocate" ]; then
    dest="\${2#/=}"
    shift
  fi
  shift
done
mkdir -p "\$dest" && cp -R '$content/.' "\$dest"
"""
        }
        tool.setExecutable(true)
    }
}
