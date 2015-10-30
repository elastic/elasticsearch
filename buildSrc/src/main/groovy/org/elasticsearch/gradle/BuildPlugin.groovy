/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle

import org.elasticsearch.gradle.precommit.PrecommitTasks
import org.gradle.api.GradleException
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.util.VersionNumber

/**
 * Encapsulates build configuration for elasticsearch projects.
 */
class BuildPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        globalBuildInfo(project)
        project.pluginManager.apply('java')
        project.pluginManager.apply('carrotsearch.randomized-testing')
        // these plugins add lots of info to our jars
        project.pluginManager.apply('nebula.info-broker')
        project.pluginManager.apply('nebula.info-basic')
        project.pluginManager.apply('nebula.info-java')
        project.pluginManager.apply('nebula.info-scm')
        project.pluginManager.apply('nebula.info-jar')

        configureCompile(project)
        configureJarManifest(project)
        configureTest(project)
        PrecommitTasks.configure(project)
    }

    static void globalBuildInfo(Project project) {
        if (project.rootProject.ext.has('buildChecksDone') == false) {
            // enforce gradle version
            VersionNumber gradleVersion = VersionNumber.parse(project.gradle.gradleVersion)
            if (gradleVersion.major < 2 || gradleVersion.major == 2 && gradleVersion.minor < 6) {
                throw new GradleException('Gradle 2.6 or above is required to build elasticsearch')
            }

            // Build debugging info
            println '======================================='
            println 'Elasticsearch Build Hamster says Hello!'
            println '======================================='
            println "  Gradle Version : ${project.gradle.gradleVersion}"
            println "  JDK Version    : ${System.getProperty('java.runtime.version')} (${System.getProperty('java.vendor')})"
            println "  OS Info        : ${System.getProperty('os.name')} ${System.getProperty('os.version')} (${System.getProperty('os.arch')})"
            project.rootProject.ext.buildChecksDone = true
        }
    }

    /** Adds compiler settings to the project */
    static void configureCompile(Project project) {
        project.afterEvaluate {
            // fail on all javac warnings
            project.tasks.withType(JavaCompile) {
                options.compilerArgs << '-Werror' << '-Xlint:all' << '-Xdoclint:all/private' << '-Xdoclint:-missing'
                options.encoding = 'UTF-8'
            }
        }
    }

    /** Adds additional manifest info to jars */
    static void configureJarManifest(Project project) {
        project.afterEvaluate {
            project.tasks.withType(Jar) { Jar jarTask ->
                manifest {
                    attributes('X-Compile-Elasticsearch-Version': ElasticsearchProperties.version,
                               'X-Compile-Lucene-Version': ElasticsearchProperties.luceneVersion)
                }
            }
        }
    }

    /** Returns a closure of common configuration shared by unit and integration tests. */
    static Closure commonTestConfig(Project project) {
        return {
            jvm System.getProperty("java.home") + File.separator + 'bin' + File.separator + 'java'
            parallelism System.getProperty('tests.jvms', 'auto')

            // TODO: why are we not passing maxmemory to junit4?
            jvmArg '-Xmx' + System.getProperty('tests.heap.size', '512m')
            jvmArg '-Xms' + System.getProperty('tests.heap.size', '512m')
            if (JavaVersion.current().isJava7()) {
                // some tests need a large permgen, but that only exists on java 7
                jvmArg '-XX:MaxPermSize=128m'
            }
            jvmArg '-XX:MaxDirectMemorySize=512m'
            jvmArg '-XX:+HeapDumpOnOutOfMemoryError'
            File heapdumpDir = new File(project.buildDir, 'heapdump')
            heapdumpDir.mkdirs()
            jvmArg '-XX:HeapDumpPath=' + heapdumpDir

            // we use './temp' since this is per JVM and tests are forbidden from writing to CWD
            systemProperty 'java.io.tmpdir', './temp'
            systemProperty 'java.awt.headless', 'true'
            systemProperty 'tests.maven', 'true' // TODO: rename this once we've switched to gradle!
            systemProperty 'tests.artifact', project.name
            systemProperty 'tests.task', path
            systemProperty 'tests.security.manager', 'true'
            // default test sysprop values
            systemProperty 'tests.ifNoTests', 'fail'
            systemProperty 'es.logger.level', 'WARN'
            for (Map.Entry<String, String> property : System.properties.entrySet()) {
                if (property.getKey().startsWith('tests.') ||
                    property.getKey().startsWith('es.')) {
                    systemProperty property.getKey(), property.getValue()
                }
            }

            // System assertions (-esa) are disabled for now because of what looks like a
            // JDK bug triggered by Groovy on JDK7. We should look at re-enabling system
            // assertions when we upgrade to a new version of Groovy (currently 2.4.4) or
            // require JDK8. See https://issues.apache.org/jira/browse/GROOVY-7528.
            enableSystemAssertions false

            testLogging {
                slowTests {
                    heartbeat 10
                    summarySize 5
                }
                stackTraceFilters {
                    // custom filters: we carefully only omit test infra noise here
                    contains '.SlaveMain.'
                    regex(/^(\s+at )(org\.junit\.)/)
                    // also includes anonymous classes inside these two:
                    regex(/^(\s+at )(com\.carrotsearch\.randomizedtesting\.RandomizedRunner)/)
                    regex(/^(\s+at )(com\.carrotsearch\.randomizedtesting\.ThreadLeakControl)/)
                    regex(/^(\s+at )(com\.carrotsearch\.randomizedtesting\.rules\.)/)
                    regex(/^(\s+at )(org\.apache\.lucene\.util\.TestRule)/)
                    regex(/^(\s+at )(org\.apache\.lucene\.util\.AbstractBeforeAfterRule)/)
                }
                outputMode System.getProperty('tests.output', 'onerror')
            }

            balancers {
                executionTime cacheFilename: ".local-${project.version}-${name}-execution-times.log"
            }

            listeners {
                junitReport()
            }

            exclude '**/*$*.class'
        }
    }

    /** Configures the test task */
    static Task configureTest(Project project) {
        Task test = project.tasks.getByName('test')
        test.configure(commonTestConfig(project))
        test.configure {
            include '**/*Tests.class'
        }
        return test
    }
}
