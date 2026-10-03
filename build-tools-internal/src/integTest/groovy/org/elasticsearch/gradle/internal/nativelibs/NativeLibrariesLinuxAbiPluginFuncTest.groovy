/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.nativelibs

import spock.lang.IgnoreIf
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.testkit.runner.TaskOutcome

class NativeLibrariesLinuxAbiPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<NativeLibrariesLinuxAbiPlugin> pluginClassUnderTest = NativeLibrariesLinuxAbiPlugin.class

    private static final String COMPATIBLE_OBJDUMP_OUTPUT = """
Version References:
  required from libstdc++.so.6:
    0x08922974 0x00 03 GLIBCXX_3.4
  required from libc.so.6:
    0x06969197 0x00 02 GLIBC_2.17
"""

    def "registers verify task and wires it to check"() {
        given:
        buildFile << """
        apply plugin: 'base'
        """

        when:
        def result = gradleRunner("tasks", "--group", "verification").build()

        then:
        result.output.contains("verifyNativeLibrariesLinuxAbi")
    }

    def "applies RHEL 8 policy defaults"() {
        given:
        buildFile << """
        tasks.named('verifyNativeLibrariesLinuxAbi').configure { verify ->
          assert verify.maxGlibcVersion.get() == '2.28'
          assert verify.maxGlibcxxVersion.get() == '3.4.25'
        }
        """

        when:
        def result = gradleRunner("help").build()

        then:
        result.output.contains("BUILD SUCCESSFUL")
    }

    def "check depends on verifyNativeLibrariesLinuxAbi"() {
        when:
        def result = gradleRunner("check", "-m").build()

        then:
        result.output.contains("verifyNativeLibrariesLinuxAbi")
    }

    @IgnoreIf({ os.isLinux() == false })
    def "succeeds for compatible linux shared libraries and writes marker"() {
        given:
        configureVerifyTask(["platform/linux-aarch64/libok.so"], compatibleObjdumpScript())
        linuxSharedLibrary("platform/linux-aarch64/libok.so")

        when:
        def result = gradleRunner("verifyNativeLibrariesLinuxAbi").build()

        then:
        result.task(":verifyNativeLibrariesLinuxAbi").outcome == TaskOutcome.SUCCESS
        file("build/markers/verify-native-libraries-linux-abi.ok").exists()
        file("build/markers/verify-native-libraries-linux-abi.ok").text.trim() == "ok"
    }

    @IgnoreIf({ os.isLinux() == false })
    def "fails when a linux shared library exceeds the minimum glibcxx ABI"() {
        given:
        configureVerifyTask(["platform/linux-aarch64/libbroken.so"], objdumpScript("""
Version References:
  required from libstdc++.so.6:
    0x0297f842 0x00 03 GLIBCXX_3.4.32
"""))
        linuxSharedLibrary("platform/linux-aarch64/libbroken.so")

        when:
        def result = gradleRunner("verifyNativeLibrariesLinuxAbi").buildAndFail()

        then:
        result.task(":verifyNativeLibrariesLinuxAbi").outcome == TaskOutcome.FAILED
        result.output.contains("GLIBCXX_3.4.32")
        result.output.contains("platform/linux-aarch64/libbroken.so")
    }

    @IgnoreIf({ os.isLinux() == false })
    def "fails when a linux shared library exceeds the minimum glibc ABI"() {
        given:
        configureVerifyTask(["platform/linux-x64/libbroken.so"], objdumpScript("""
Version References:
  required from libc.so.6:
    0x069691b8 0x00 02 GLIBC_2.38
"""))
        linuxSharedLibrary("platform/linux-x64/libbroken.so")

        when:
        def result = gradleRunner("verifyNativeLibrariesLinuxAbi").buildAndFail()

        then:
        result.task(":verifyNativeLibrariesLinuxAbi").outcome == TaskOutcome.FAILED
        result.output.contains("GLIBC_2.38")
        result.output.contains("platform/linux-x64/libbroken.so")
    }

    @IgnoreIf({ os.isLinux() == false })
    def "accepts bare maxGlibcVersion policy and rejects glibc above boundary"() {
        given:
        buildFile << """
        apply plugin: 'base'

        tasks.named('verifyNativeLibrariesLinuxAbi').configure {
          nativeLibraries.from('platform/linux-aarch64/libboundary.so')
          maxGlibcVersion.set('2.28')
          objdumpExecutable.set("${projectDir}/fake-objdump")
        }
        """
        linuxSharedLibrary("platform/linux-aarch64/libboundary.so")
        file("fake-objdump").text = objdumpScript("""
Version References:
  required from libc.so.6:
    0x06969194 0x00 02 GLIBC_2.29
""")
        file("fake-objdump").setExecutable(true)

        when:
        def result = gradleRunner("verifyNativeLibrariesLinuxAbi").buildAndFail()

        then:
        result.output.contains("GLIBC_2.29")
    }

    @IgnoreIf({ os.isLinux() == false })
    def "ignores non-linux libraries in the platform tree"() {
        given:
        configureVerifyTask(["platform/darwin-aarch64/libvec.dylib"], compatibleObjdumpScript())
        file("platform/darwin-aarch64").mkdirs()
        file("platform/darwin-aarch64/libvec.dylib").bytes = [0x00] as byte[]

        when:
        def result = gradleRunner("verifyNativeLibrariesLinuxAbi").build()

        then:
        result.task(":verifyNativeLibrariesLinuxAbi").outcome == TaskOutcome.SUCCESS
        file("build/markers/verify-native-libraries-linux-abi.ok").exists()
    }

    @IgnoreIf({ os.isLinux() == false })
    def "reports all violating libraries"() {
        given:
        configureVerifyTask(
            ["platform/linux-aarch64/libbad1.so", "platform/linux-x64/libbad2.so"],
            objdumpScript("""
Version References:
  required from libstdc++.so.6:
    0x0297f842 0x00 03 GLIBCXX_3.4.32
  required from libc.so.6:
    0x069691b8 0x00 02 GLIBC_2.38
""")
        )
        linuxSharedLibrary("platform/linux-aarch64/libbad1.so")
        linuxSharedLibrary("platform/linux-x64/libbad2.so")

        when:
        def result = gradleRunner("verifyNativeLibrariesLinuxAbi").buildAndFail()

        then:
        result.output.contains("platform/linux-aarch64/libbad1.so")
        result.output.contains("platform/linux-x64/libbad2.so")
        result.output.contains("GLIBCXX_3.4.32")
        result.output.contains("GLIBC_2.38")
    }

    @IgnoreIf({ os.isLinux() == false })
    def "fails when objdump cannot inspect a shared library"() {
        given:
        configureVerifyTask(["platform/linux-aarch64/libbroken.so"], failingInspectObjdumpScript())
        linuxSharedLibrary("platform/linux-aarch64/libbroken.so")

        when:
        def result = gradleRunner("verifyNativeLibrariesLinuxAbi").buildAndFail()

        then:
        result.task(":verifyNativeLibrariesLinuxAbi").outcome == TaskOutcome.FAILED
        result.output.contains("Failed to inspect")
        result.output.contains("libbroken.so")
    }

    private void configureVerifyTask(List<String> libraryPaths, String fakeObjdumpScript) {
        String paths = libraryPaths.collect { "'${it}'" }.join(", ")
        buildFile << """
        apply plugin: 'base'

        tasks.named('verifyNativeLibrariesLinuxAbi').configure {
          nativeLibraries.from(${paths})
          objdumpExecutable.set("${projectDir}/fake-objdump")
        }
        """
        file("fake-objdump").text = fakeObjdumpScript
        file("fake-objdump").setExecutable(true)
    }

    private static String compatibleObjdumpScript() {
        return objdumpScript(COMPATIBLE_OBJDUMP_OUTPUT)
    }

    private static String objdumpScript(String output) {
        return """#!/bin/sh
if [ "\$1" = "--version" ]; then
  exit 0
fi
cat <<'EOF'
${output}EOF
"""
    }

    private static String failingInspectObjdumpScript() {
        return """#!/bin/sh
if [ "\$1" = "--version" ]; then
  exit 0
fi
exit 1
"""
    }

    private void linuxSharedLibrary(String relativePath) {
        def parent = file(relativePath).parentFile
        parent.mkdirs()
        file(relativePath).bytes = [0x7f, 0x45, 0x4c, 0x46] as byte[]
    }
}
