/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

import java.util.zip.ZipEntry
import java.util.zip.ZipFile

class EmbeddedProviderPluginFuncTest extends AbstractGradleFuncTest {

    def "embedded provider fat jar is cacheable as classpath input"() {
        given:
        def implBuild = subProject(":impl")
        implBuild << """
            plugins {
              id 'elasticsearch.java'
            }
            group = 'org.acme'
            version = '1.0'
        """
        file("impl/src/main/java/org/acme/Impl.java") << """
            package org.acme;
            public class Impl {
              public static String message() { return "hello"; }
            }
        """.stripIndent().stripTrailing()

        buildFile << """
            plugins {
              id 'elasticsearch.java'
              id 'elasticsearch.embedded-providers'
            }
            group = 'org.acme'
            version = '1.0'

            embeddedProviders {
              impl 'test', project(':impl')
            }

            import org.gradle.api.DefaultTask
            import org.gradle.api.file.ConfigurableFileCollection
            import org.gradle.api.file.RegularFileProperty
            import org.gradle.api.tasks.CacheableTask
            import org.gradle.api.tasks.Classpath
            import org.gradle.api.tasks.OutputFile
            import org.gradle.api.tasks.TaskAction
            import java.net.URL
            import java.net.URLClassLoader

            @CacheableTask
            abstract class UseEmbeddedProvidersOnClasspath extends DefaultTask {
              @Classpath
              abstract ConfigurableFileCollection getClasspath()

              @OutputFile
              abstract RegularFileProperty getOutputFile()

              @OutputFile
              abstract RegularFileProperty getJarPathFile()

              @TaskAction
              void executeTask() {
                def urls = classpath.files.toList().sort { it.absolutePath }.collect { it.toURI().toURL() } as URL[]
                def classpathFiles = classpath.files.toList().sort { it.absolutePath }
                jarPathFile.get().asFile.text = classpathFiles*.absolutePath.join(System.lineSeparator()) + System.lineSeparator()
                def resourceName = null
                for (def f : classpathFiles) {
                  java.util.zip.ZipFile zipFile = new java.util.zip.ZipFile(f)
                  try {
                    def entries = zipFile.entries()
                    while (entries.hasMoreElements()) {
                      def entry = entries.nextElement()
                      def name = entry.getName()
                      if (name.startsWith("IMPL-JARS/test/") && name.endsWith("org/acme/Impl.class")) {
                        resourceName = name
                        break
                      }
                    }
                  } finally {
                    zipFile.close()
                  }
                  if (resourceName != null) {
                    break
                  }
                }

                assert resourceName != null : "Expected embedded impl class resource to exist on the jar classpath"

                def digestBytes
                URLClassLoader loader = new URLClassLoader(urls, (ClassLoader) null)
                try {
                  def stream = loader.getResourceAsStream(resourceName)
                  assert stream != null : "Expected embedded impl class resource to exist on the jar classpath"
                  stream.withCloseable {
                    digestBytes = java.security.MessageDigest.getInstance("SHA-256").digest(it.bytes)
                  }
                } finally {
                  loader.close()
                }
                assert digestBytes != null : "Expected embedded impl class resource to exist on the jar classpath"
                outputFile.get().asFile.text = digestBytes.encodeHex().toString()
              }
            }

            tasks.register("useEmbeddedProvidersOnClasspath", UseEmbeddedProvidersOnClasspath) {
              dependsOn(tasks.named("jar"))
              classpath.from(tasks.named("jar").flatMap { it.archiveFile })
              outputFile.set(layout.buildDirectory.file("useEmbeddedProvidersOnClasspath/output.txt"))
              jarPathFile.set(layout.buildDirectory.file("useEmbeddedProvidersOnClasspath/jar-path.txt"))
            }
        """
        file("src/main/java/org/acme/Main.java") << """
            package org.acme;
            public class Main { }
        """.stripIndent().stripTrailing()

        when:
        def firstRun = gradleRunner("clean", "useEmbeddedProvidersOnClasspath", "--build-cache", "-g", gradleUserHome).build()

        then:
        firstRun.task(":useEmbeddedProvidersOnClasspath").outcome == TaskOutcome.SUCCESS

        and:
        firstRun.task(":jar")?.outcome in [TaskOutcome.SUCCESS, TaskOutcome.FROM_CACHE]
        firstRun.task(":generateImplProviderImpl")?.outcome in [TaskOutcome.SUCCESS, TaskOutcome.UP_TO_DATE, TaskOutcome.FROM_CACHE]
        String jarPath = file("build/useEmbeddedProvidersOnClasspath/jar-path.txt").readLines().find { it?.trim() }
        assert jarPath != null : "Expected useEmbeddedProvidersOnClasspath to record jar path"
        File jar = new File(jarPath.trim())
        jar.exists()

        and: "impl jar content is embedded but impl MANIFEST.MF is not"
        boolean hasEmbeddedImplClass = false
        boolean hasEmbeddedImplManifest = false

        try (ZipFile zipFile = new ZipFile(jar)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries()
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement()
                String name = entry.getName()
                if (name.startsWith("IMPL-JARS/test/") && name.endsWith(".class")) {
                    hasEmbeddedImplClass = true
                }
                if (name.startsWith("IMPL-JARS/test/") && name.endsWith("META-INF/MANIFEST.MF")) {
                    hasEmbeddedImplManifest = true
                }
            }
        }

        assert hasEmbeddedImplClass : "Expected at least one embedded impl .class under IMPL-JARS/test/"
        assert hasEmbeddedImplManifest == false : "Did not expect IMPL-JARS/test/**/META-INF/MANIFEST.MF in output jar"

        when:
        def secondRun = gradleRunner("clean", "useEmbeddedProvidersOnClasspath", "--build-cache", "-g", gradleUserHome).build()

        then:
        secondRun.task(":useEmbeddedProvidersOnClasspath").outcome == TaskOutcome.FROM_CACHE
    }
}

