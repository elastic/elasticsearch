/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.checks;

import org.codehaus.groovy.control.CompilationFailedException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ExecutionClosureScannerTests {
    @Test
    public void testExecutionContextsAndLocations() {
        var findings = scan("""
            tasks.register('probe') {
              doFirst { file('Versions.asciidoc') }
              doLast { sourceSets.main.output }
              onlyIf('reason') { project.hasProperty('enabled') && gradle.startParameter.offline && rootProject.name }
            }
            """);
        assertEquals(List.of("file()", "sourceSets", "project", "gradle", "rootProject"), references(findings));
        assertEquals(List.of(2, 3, 4, 4, 4), findings.stream().map(ExecutionClosureScanner.Finding::line).toList());
        assertEquals(13, findings.get(0).column());
    }

    @Test
    public void testCapturesAndTextAreNotModelReferences() {
        var findings = scan("""
            def capturedName = project.name
            def capturedFile = file('data')
            def capturedSources = sourceSets.main.output
            tasks.register('safe') {
              def offline = gradle.startParameter.offline
              doFirst { println capturedFile }
              doLast {
                // project rootProject gradle sourceSets file('ignored')
                println 'project rootProject gradle sourceSets file()'
                println(/project{ gradle }/)
                println [project: capturedName, gradle: offline, sourceSets: capturedSources]
                println "${capturedName} ${capturedFile}"
              }
              onlyIf { !offline }
            }
            """);
        assertTrue(findings.toString(), findings.isEmpty());
    }

    @Test
    public void testLexicalShadowingAndUnrelatedReceivers() {
        assertTrue(scan("""
            def project = [name: 'captured']
            def file = { value -> value }
            tasks.register('safe') {
              doLast { gradle ->
                def rootProject = [:]
                def sourceSets = [:]
                println project.name
                file('data')
                println gradle
                println rootProject
                println sourceSets
                other.project
                other.gradle
                other.sourceSets
                other.file('data')
                [1].each { project -> println project }
              }
            }
            other.doLast { project.name }
            other.onlyIf { gradle.startParameter.offline }
            """).isEmpty());
    }

    @Test
    public void testSelfNamedInitializersResolveBeforeLocalShadowing() {
        var findings = scan("""
            tasks.register('probe') {
              doLast {
                def project = project
                def gradle = gradle
                def sourceSets = sourceSets
                def file = file('data')
                println project.name
                println gradle
                println sourceSets
                file('other')
              }
            }
            """);
        assertEquals(List.of("project", "gradle", "sourceSets", "file()"), references(findings));
        assertEquals(List.of(3, 4, 5, 6), findings.stream().map(ExecutionClosureScanner.Finding::line).toList());
    }

    @Test
    public void testNestedClosuresInterpolationAndScopeRestoration() {
        var findings = scan("""
            tasks.named('probe').configure {
              doLast {
                [1].each { project -> println project }
                [1].each { println "${project.name}" }
                { -> gradle.startParameter.offline }.call()
                if (true) { def sourceSets = []; println sourceSets }
                println sourceSets.main.output
              }
            }
            """);
        assertEquals(List.of("project", "gradle", "sourceSets"), references(findings));
    }

    @Test
    public void testExplicitTaskAndScriptReceivers() {
        var findings = scan("""
            def probe = tasks.register('probe')
            probe.configure { task ->
              task.onlyIf { task.project.name }
              task.doFirst { this.file('data') }
              task.doLast { this.gradle.startParameter.offline }
            }
            tasks.withType(Test).configureEach { it.onlyIf { it.project.name } }
            """);
        assertEquals(List.of("project", "file()", "gradle", "project"), references(findings));
    }

    @Test
    public void testLoopCatchAndMethodParametersDoNotLeak() {
        var findings = scan("""
            void configureProbe(Task task, String project) {
              task.doLast {
                println project
                for (gradle in []) { println gradle }
                try { println 'ok' } catch (Exception rootProject) { println rootProject }
                println gradle
                println rootProject
              }
            }
            """);
        assertEquals(List.of("gradle", "rootProject"), references(findings));
    }

    @Test
    public void testOtherCallbacksAndConfigurationClosuresAreOutsideScope() {
        assertTrue(scan("""
            tasks.register('safe') {
              println project.name
              outputs.upToDateWhen { project.name }
              eachFile { file('data'); println sourceSets }
            }
            """).isEmpty());
    }

    @Test
    public void testLegacyTaskDeclarationsHaveExecutionClosures() {
        for (String declaration : List.of("task probe", "task probe(type: Exec)", "task('probe')")) {
            for (String action : List.of("doFirst", "doLast", "onlyIf")) {
                var findings = scan(declaration + " {\n  " + action + " { project.name }\n}");
                assertEquals(declaration + " / " + action, List.of("project"), references(findings));
                assertEquals(2, findings.get(0).line());
            }
        }
    }

    @Test
    public void testLegacyTaskConfigurationValuesCanBeCaptured() {
        for (String declaration : List.of("task probe", "task probe(type: Exec)", "task('probe')")) {
            assertTrue(scan(declaration + """
                 {
                  def name = project.name
                  def target = file('data')
                  def online = !gradle.startParameter.offline
                  doLast { println name; println target }
                  onlyIf { online }
                }
                """).isEmpty());
        }
    }

    @Test
    public void testNestedLegacyDeclarationDoesNotRecognizeUnrelatedCalls() {
        assertTrue(scan("""
            foo { doLast { project.name } }
            other.foo { doLast { project.name } }
            other.task(foo { doLast { project.name } })
            task(other.foo { doLast { project.name } })
            task(foo(bar { doLast { project.name } }))
            task(foo(42) { doLast { project.name } })
            def task = { value -> value }
            task(foo { doLast { project.name } })
            """).isEmpty());
    }

    @Test
    public void testQualifiedModelAccessIsCountedAtTheReceiverOnly() {
        var findings = scan("""
            tasks.register('probe') {
              doLast {
                project.file('data')
                project.sourceSets.main.output
                rootProject.file('data')
                rootProject.sourceSets.main.output
                this.file('data')
                this.project.sourceSets.main.output
              }
            }
            """);
        assertEquals(List.of("project", "project", "rootProject", "rootProject", "file()", "project"), references(findings));
    }

    @Test
    public void testBrokenScriptIsNotSilentlySkipped() {
        assertThrows(CompilationFailedException.class, () -> scan("tasks.register('broken') { doLast { project.name }"));
    }

    private static List<ExecutionClosureScanner.Finding> scan(String script) {
        return ExecutionClosureScanner.scan("fixture/build.gradle", script);
    }

    private static List<String> references(List<ExecutionClosureScanner.Finding> findings) {
        return findings.stream().map(ExecutionClosureScanner.Finding::reference).toList();
    }
}
