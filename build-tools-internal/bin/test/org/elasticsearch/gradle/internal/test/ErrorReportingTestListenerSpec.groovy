/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test

import org.elasticsearch.gradle.OS
import org.gradle.api.logging.Logger
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.TestDescriptor
import org.gradle.api.tasks.testing.TestOutputEvent
import org.gradle.api.tasks.testing.TestResult
import org.gradle.api.tasks.testing.logging.TestLoggingContainer
import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.lang.TempDir

class ErrorReportingTestListenerSpec extends Specification {

    @TempDir
    File tempDir

    File outputDir
    ErrorReportingTestListener listener

    def setup() {
        outputDir = new File(tempDir, "output")
        assert outputDir.mkdirs()
        def testTask = Stub(Test) {
            getTestLogging() >> Stub(TestLoggingContainer)
            getLogger() >> Stub(Logger)
        }
        listener = new ErrorReportingTestListener(testTask, outputDir)
    }

    def "suite output is captured in a file named after the class"() {
        given:
        def suite = suiteDescriptor("com.example.FooTests", "com.example.FooTests")

        when:
        listener.onOutput(suite, outputEvent("hi"))

        then:
        new File(outputDir, "com.example.FooTests.out").isFile()
    }

    def "suites without a class name never produce null.out"() {
        given:
        def suite = suiteDescriptor(null, "Gradle Test Run :x-pack:plugin:esql:internalClusterTest")

        when:
        listener.onOutput(suite, outputEvent("root output"))

        then:
        new File(outputDir, "null.out").exists() == false
        outputDir.listFiles().length == 1
        outputDir.listFiles()[0].name != "null.out"
    }

    def "distinct suites without class names map to distinct files"() {
        given:
        def suiteA = suiteDescriptor(null, "Gradle Test Run :a")
        def suiteB = suiteDescriptor(null, "Gradle Test Run :b")

        when:
        listener.onOutput(suiteA, outputEvent("a"))
        listener.onOutput(suiteB, outputEvent("b"))

        then:
        outputDir.listFiles().length == 2
    }

    @IgnoreIf({ OS.current() == OS.WINDOWS })
    def "afterSuite tolerates an output file that was concurrently removed"() {
        // Simulates the race where a concurrent afterSuite for an equivalent Descriptor
        // has already closed and deleted the shared output file before this afterSuite reads it.
        given:
        def suite = suiteDescriptor("com.example.FooTests", "com.example.FooTests")
        listener.onOutput(suite, outputEvent("captured output"))
        def outputFile = new File(outputDir, "com.example.FooTests.out")
        assert outputFile.isFile()
        assert outputFile.delete()

        when:
        listener.afterSuite(suite, failedResult())

        then:
        noExceptionThrown()
    }

    def "afterSuite without captured output is a no-op"() {
        given:
        def suite = suiteDescriptor("com.example.FooTests", "com.example.FooTests")

        when:
        listener.afterSuite(suite, failedResult())

        then:
        noExceptionThrown()
        outputDir.listFiles().length == 0
    }

    def "afterTest records only failing tests"() {
        given:
        def parent = suiteDescriptor("com.example.FooTests", "com.example.FooTests")
        def failing = testDescriptor("testBoom", "com.example.FooTests", parent)
        def passing = testDescriptor("testOk", "com.example.FooTests", parent)

        when:
        listener.afterTest(failing, failedResult())
        listener.afterTest(passing, succeededResult())

        then:
        listener.failedTests*.fullName == ["com.example.FooTests.testBoom"]
    }

    private TestDescriptor suiteDescriptor(String className, String name) {
        return Stub(TestDescriptor) {
            getClassName() >> className
            getName() >> name
            isComposite() >> true
            getParent() >> null
        }
    }

    private TestDescriptor testDescriptor(String name, String className, TestDescriptor parent) {
        return Stub(TestDescriptor) {
            getClassName() >> className
            getName() >> name
            isComposite() >> false
            getParent() >> parent
        }
    }

    private TestOutputEvent outputEvent(String message) {
        return Stub(TestOutputEvent) {
            getMessage() >> message
            getDestination() >> TestOutputEvent.Destination.StdOut
        }
    }

    private TestResult failedResult() {
        return Stub(TestResult) {
            getResultType() >> TestResult.ResultType.FAILURE
            getExceptions() >> []
        }
    }

    private TestResult succeededResult() {
        return Stub(TestResult) {
            getResultType() >> TestResult.ResultType.SUCCESS
            getExceptions() >> []
        }
    }
}
