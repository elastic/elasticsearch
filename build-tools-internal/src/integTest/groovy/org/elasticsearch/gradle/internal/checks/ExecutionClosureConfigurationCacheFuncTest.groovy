/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.checks

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.elasticsearch.gradle.internal.fixtures.ExecutionActionFixture
import org.gradle.testkit.runner.GradleRunner
import spock.lang.Unroll

import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

/** Executes the demonstrated failure modes, not just a dry run of task registration. */
class ExecutionClosureConfigurationCacheFuncTest extends AbstractGradleFuncTest {
    @Unroll
    def "execution reference #reference in #context for #declaration fails validation and configuration cache"() {
        given:
        buildFile.text = """
plugins { id 'java' }
$declaration {
    $context { $expression }
}
"""

        String fixture = buildFile.text

        when:
        def result = probeRunner().buildAndFail()

        then:
        result.output.contains(diagnostic)
        result.output.contains('BUILD FAILED')

        when: "the validator reads the identical script that failed during real execution"
        def validation = validatorRunner(fixture).buildAndFail()

        then:
        validation.output.contains('execution-closure-project-reference')
        validation.output.contains('fixture/build.gradle:4')

        where:
        reference     | context   | declaration                   | expression                              | diagnostic
        'file()'      | 'doFirst' | "tasks.register('probe')"     | "println file('Versions.asciidoc')"     | 'Gradle script object'
        'sourceSets'  | 'doLast'  | "tasks.register('probe')"     | 'println sourceSets.main.output'        | "unknown property 'sourceSets'"
        'project'     | 'doLast'  | "tasks.register('probe')"     | 'println project.name'                  | 'Task.project'
        'rootProject' | 'doLast'  | "tasks.register('probe')"     | 'println rootProject.name'              | "invocation of 'rootProject'"
        'gradle'      | 'onlyIf'  | "tasks.register('probe')"     | 'gradle.startParameter.offline == false' | "invocation of 'gradle'"
        'project'     | 'onlyIf'  | "tasks.register('probe')"     | "project.hasProperty('enabled')"        | "invocation of 'project'"
        'project'     | 'doLast'  | 'task probe'                  | 'println project.name'                  | 'Task.project'
        'project'     | 'doLast'  | "tasks.register('probe')"     | 'def project = project; println project.name' | 'Task.project'
        'project'     | 'onlyIf'  | 'task probe(type: Exec)'      | "project.hasProperty('enabled')"        | "invocation of 'project'"
    }

    def "captured values in a legacy task pass validation and run twice with configuration cache reuse"() {
        given:
        buildFile.text = '''
plugins { id 'java' }
def capturedFile = file('data.txt')
def capturedName = project.name
def capturedSources = sourceSets.main.output.files.collect { it.name }.join(',')
def online = !gradle.startParameter.offline
task probe {
    onlyIf { online }
    doFirst { capturedFile.text = capturedName }
    doLast {
        def project = [name: 'local']
        println "safe ${project.name} ${capturedSources}"
        [project: capturedName, gradle: online].each { key, value -> println "$key=$value" }
        assert capturedFile.text == capturedName
    }
}
'''

        String fixture = buildFile.text

        when:
        def first = probeRunner().build()
        def second = probeRunner().build()

        then:
        first.output.contains('Configuration cache entry stored.')
        second.output.contains('Reusing configuration cache.')
        second.output.contains('safe local')
        file('data.txt').text == 'hello-world'

        when:
        def validation = validatorRunner(fixture).build()

        then:
        validation.output.contains('BUILD SUCCESSFUL')
    }

    def "the compiled bytecode fixture demonstrates a real unsafe execution capture"() {
        given:
        fixtureJar()
        buildFile.text = '''
buildscript { dependencies { classpath files('fixture.jar') } }
import org.elasticsearch.gradle.internal.fixtures.ExecutionActionFixture
tasks.register('probe') { new ExecutionActionFixture().unsafe(it, project, gradle) }
'''

        when:
        def result = probeRunner().buildAndFail()

        then:
        result.output.contains('org.gradle.api.Project')
        result.output.contains('configuration cache')
        result.output.contains('cannot serialize')
    }

    def "configuration Actions and Java value captures remain valid on cache reuse"() {
        given:
        fixtureJar()
        buildFile.text = '''
buildscript { dependencies { classpath files('fixture.jar') } }
import org.elasticsearch.gradle.internal.fixtures.ExecutionActionFixture
def probe = tasks.register('probe').get()
new ExecutionActionFixture().safe(project, probe)
probe.dependsOn('capturedValue')
'''

        when:
        probeRunner().build()
        def second = probeRunner().build()

        then:
        second.output.contains('Reusing configuration cache.')
        second.output.contains('no model capture')
    }

    private GradleRunner validatorRunner(String fixture) {
        file('fixture/build.gradle').text = fixture
        buildFile.text = '''
plugins { id 'elasticsearch.global-build-info' }
import org.elasticsearch.gradle.internal.checks.ValidateBuildGradleScriptsTask
tasks.register('validateFixture', ValidateBuildGradleScriptsTask) {
    scriptFiles.from(layout.projectDirectory.file('fixture/build.gradle'))
    ignore.set([:])
}
'''
        gradleRunner('validateFixture')
    }

    private GradleRunner probeRunner() {
        // Deliberately no ES plugin classpath or wrapper: these probes isolate Gradle's actual
        // execution/cache diagnostics, including builds expected to fail compatibility checking.
        GradleRunner.create().withProjectDir(projectDir)
            .withArguments('probe', '--configuration-cache', '--rerun-tasks', '--stacktrace', '--warning-mode=fail')
    }

    private void fixtureJar() {
        file('fixture.jar').withOutputStream { stream ->
            new JarOutputStream(stream).withCloseable { jar ->
                ['', '$1', '$Unrelated', '$ValueAction'].each { suffix ->
                    String path = ExecutionActionFixture.name.replace('.', '/') + suffix + '.class'
                    jar.putNextEntry(new JarEntry(path))
                    ExecutionActionFixture.classLoader.getResourceAsStream(path).withCloseable { bytes ->
                        jar << bytes
                    }
                    jar.closeEntry()
                }
            }
        }
    }
}
