/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.apache.commons.io.IOUtils
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.gradle.testkit.runner.TaskOutcome
import org.junit.ClassRule
import spock.lang.Shared

import java.nio.charset.StandardCharsets
import java.util.zip.ZipEntry
import java.util.zip.ZipFile

import static org.elasticsearch.gradle.fixtures.TestClasspathUtils.setupJarHellJar

class BuildPluginFuncTest extends AbstractGradleFuncTest {

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture()

    def EXAMPLE_LICENSE = """\
        Redistribution and use in source and binary forms, with or without
        modification, are permitted provided that the following conditions
        are met:

         1. Redistributions of source code must retain the above copyright
            notice, this list of conditions and the following disclaimer.
         2. Redistributions in binary form must reproduce the above copyright
            notice, this list of conditions and the following disclaimer in the
            documentation and/or other materials provided with the distribution.
         3. The name of the author may not be used to endorse or promote products
            derived from this software without specific prior written permission.

        THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
        IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
        OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
        IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
        INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
        NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
        DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
        THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
        (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
        THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.""".stripIndent()

    def setup() {
        configurationCacheCompatible = false
        buildFile << """
        plugins {
          id 'java'
          id 'elasticsearch.global-build-info'
        }

        apply plugin:'elasticsearch.build'
        group = 'org.acme'
        description = "some example project"

        repositories {
          maven {
            name = "local-test"
            url = file("local-repo")
            metadataSources {
              artifact()
            }
          }
          mavenCentral()
        }

        dependencies {
          jarHell 'org.elasticsearch:elasticsearch-core:current'
        }
        """
        file("LICENSE") << "this is a test license file"
        file("NOTICE") << "this is a test notice file"
        file('src/main/java/org/elasticsearch/SampleClass.java') << """\
          /*
           * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
           * or more contributor license agreements. Licensed under the "Elastic License
           * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
           * Public License v 1"; you may not use this file except in compliance with, at
           * your election, the "Elastic License 2.0", the "GNU Affero General Public
           * License v3.0 only", or the "Server Side Public License, v 1".
           */
          package org.elasticsearch;

          public class SampleClass {
          }
        """.stripIndent()
    }
    def "plugin can be applied"() {
        given:
        buildFile << """
            tasks.register("hello") {
              doFirst {
                println "build plugin can be applied"
              }
            }
            """
        when:
        def result = gradleRunner("hello").build()
        then:
        result.task(":hello").outcome == TaskOutcome.SUCCESS
        assertOutputContains(result.getOutput(), "build plugin can be applied")
    }

    def "packages license and notice file"() {
        given:
        buildFile << """
            licenseFile.set(file("LICENSE"))
            noticeFile.set(file("NOTICE"))
            """
        when:
        def result = gradleRunner("assemble", "-x", "generateClusterFeaturesMetadata").build()
        then:
        result.task(":assemble").outcome == TaskOutcome.SUCCESS
        file("build/distributions/hello-world.jar").exists()
        file("build/distributions/hello-world-javadoc.jar").exists()
        file("build/distributions/hello-world-sources.jar").exists()
        assertValidJar(file("build/distributions/hello-world.jar"))
    }

    def "applies checks"() {
        given:
        withVersionCatalogue()
        repository.generateJar("org.elasticsearch", "build-conventions", "unspecified", 'org.acme.CheckstyleStuff')
        repository.configureBuild(buildFile)
        setupJarHellJar(dir('local-repo/org/elasticsearch/elasticsearch-core/current/'))
        file("licenses/hamcrest-core-1.3.jar.sha1").text = "42a25dc3219429f0e5d060061f71acb49bf010a0"
        file("licenses/hamcrest-core-LICENSE.txt").text = EXAMPLE_LICENSE
        file("licenses/hamcrest-core-NOTICE.txt").text = "mock notice"
        file("licenses/junit-4.13.2.jar.sha1").text = "2973d150c0dc1fefe998f834810d68f278ea58ec"
        file("licenses/junit-LICENSE.txt").text = EXAMPLE_LICENSE
        file("licenses/junit-NOTICE.txt").text = "mock notice"
        buildFile << """
            dependencies {
              api "junit:junit:4.13.2"
              // missing classes in thirdparty audit
              api 'org.hamcrest:hamcrest-core:1.3'
            }
            licenseFile.set(file("LICENSE"))
            noticeFile.set(file("NOTICE"))

            tasks.named("forbiddenApisMain").configure {enabled = false }
            tasks.named('checkstyleMain').configure { enabled = false }
            tasks.named('loggerUsageCheck').configure { enabled = false }
            // tested elsewhere
            tasks.named('thirdPartyAudit').configure { enabled = false }
            """
        when:
        def result = gradleRunner("check").build()
        then:
        result.task(":licenseHeaders").outcome == TaskOutcome.SUCCESS
        result.task(":forbiddenPatterns").outcome == TaskOutcome.SUCCESS
        result.task(":validateModule").outcome == TaskOutcome.SUCCESS
        result.task(":splitPackagesAudit").outcome == TaskOutcome.SUCCESS
        result.task(":validateElasticPom").outcome == TaskOutcome.SUCCESS
        // disabled but check for being on the task graph
        result.task(":forbiddenApisMain").outcome == TaskOutcome.SKIPPED
        result.task(":checkstyleMain").outcome == TaskOutcome.SKIPPED
        result.task(":thirdPartyAudit").outcome == TaskOutcome.SKIPPED
        result.task(":loggerUsageCheck").outcome == TaskOutcome.SKIPPED
    }

    def assertValidJar(File jar) {
        try (ZipFile zipFile = new ZipFile(jar)) {
            ZipEntry licenseEntry = zipFile.getEntry("META-INF/LICENSE.txt")
            ZipEntry noticeEntry = zipFile.getEntry("META-INF/NOTICE.txt")

            assert licenseEntry != null : "Jar does not have META-INF/LICENSE.txt"
            assert noticeEntry != null : "Jar does not have META-INF/NOTICE.txt"
            try (InputStream license = zipFile.getInputStream(licenseEntry); InputStream notice = zipFile.getInputStream(noticeEntry)) {
                assert "this is a test license file" == IOUtils.toString(license, StandardCharsets.UTF_8.name())
                assert "this is a test notice file" == IOUtils.toString(notice, StandardCharsets.UTF_8.name())
            }
        }
        true
    }
}
