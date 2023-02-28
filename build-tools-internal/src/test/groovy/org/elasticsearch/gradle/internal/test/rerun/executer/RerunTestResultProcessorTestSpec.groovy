/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rerun.executer

import org.gradle.api.internal.tasks.testing.TestCompleteEvent
import org.gradle.api.internal.tasks.testing.TestDescriptorInternal
import org.gradle.api.internal.tasks.testing.TestResultProcessor
import org.gradle.api.internal.tasks.testing.TestStartEvent
import org.gradle.api.tasks.testing.TestFailure
import org.gradle.api.tasks.testing.TestOutputEvent
import spock.lang.Specification

class RerunTestResultProcessorTestSpec extends Specification {

    def "delegates test events"() {
        given:
        def delegate = Mock(TestResultProcessor)
        def processor = new RerunTestResultProcessor(delegate);
        def testDesciptorInternal = Mock(TestDescriptorInternal);
        def testId = "TestId"
        _ * testDesciptorInternal.getId() >> testId

        def testStartEvent = Mock(TestStartEvent)
        def testOutputEvent = Mock(TestOutputEvent)
        def testCompleteEvent = Mock(TestCompleteEvent)

        when:
        processor.started(testDesciptorInternal, testStartEvent)
        then:
        1 * delegate.started(testDesciptorInternal, testStartEvent)

        when:
        processor.output(testId, testOutputEvent)
        then:
        1 * delegate.output(testId, testOutputEvent)

        when:
        processor.completed(testId, testCompleteEvent)
        then:
        1 * delegate.completed(testId, testCompleteEvent)
    }

    def "ignores retriggered root test events"() {
        given:
        def delegate = Mock(TestResultProcessor)
        def processor = new RerunTestResultProcessor(delegate);
        def rootDescriptor = descriptor("rootId")
        def testStartEvent = Mock(TestStartEvent)

        when:
        processor.started(rootDescriptor, testStartEvent)
        processor.started(rootDescriptor, testStartEvent)
        then:
        1 * delegate.started(rootDescriptor, testStartEvent)
        _ * delegate.started(_, _)
    }

    def "ignores root complete event when tests not finished"() {
        given:
        def delegate = Mock(TestResultProcessor)
        def processor = new RerunTestResultProcessor(delegate);

        def rootDescriptor = descriptor("rootId")
        def rootTestStartEvent = startEvent("rootId")
        def rootCompleteEvent = Mock(TestCompleteEvent)

        def testDescriptor1 = descriptor("testId1")
        def testStartEvent1 = startEvent("testId1")
        def testCompleteEvent1 = Mock(TestCompleteEvent)

        def testDescriptor2 = descriptor("testId2")
        def testStartEvent2 = startEvent("testId2")
        def testFailure = Mock(TestFailure)

        when:
        processor.started(rootDescriptor, rootTestStartEvent)
        processor.started(testDescriptor1, testStartEvent1)
        processor.started(testDescriptor2, testStartEvent2)
        processor.failure("testId2", testFailure)
        processor.completed("rootId", rootCompleteEvent)

        then:
        1 * delegate.started(rootDescriptor, rootTestStartEvent)
        1 * delegate.started(testDescriptor1, testStartEvent1)
        1 * delegate.started(testDescriptor2, testStartEvent2)
        1 * delegate.failure("testId2", testFailure)
        0 * delegate.completed("rootId", rootCompleteEvent)

        when:
        processor.completed("testId1", testCompleteEvent1)
        processor.completed("rootId", rootCompleteEvent)

        then:
        1 * delegate.completed("testId1", testCompleteEvent1)
        1 * delegate.completed("rootId", rootCompleteEvent)
    }

    def "events for aborted tests are ignored"() {
        given:
        def delegate = Mock(TestResultProcessor)
        def processor = new RerunTestResultProcessor(delegate);
        def rootDescriptor = descriptor("rootId")
        def rootTestStartEvent = startEvent("rootId")

        def testDescriptor = descriptor("testId")
        def testStartEvent = startEvent("testId")
        def testCompleteEvent = Mock(TestCompleteEvent)

        processor.started(rootDescriptor, rootTestStartEvent)
        processor.started(testDescriptor, testStartEvent)

        when:
        processor.reset()
        processor.completed("testId", testCompleteEvent)
        then:
        0 * delegate.completed("testId", testCompleteEvent)
    }

    TestDescriptorInternal descriptor(String id) {
        def desc = Mock(TestDescriptorInternal)
        _ * desc.getId() >> id
        desc
    }

    TestStartEvent startEvent(String parentId) {
        def event = Mock(TestStartEvent)
        _ * event.getParentId() >> parentId
        event
    }

//    TestFinishEvent finishEvent(String parentId) {
//        def event = Mock(TestFinishEvent)
//        _ * event.getParentId() >> parentId
//        event
//    }
}
