/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneSourceOperatorStatusTests;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatusTests;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperatorStatusTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class DriverStatusTests extends AbstractWireSerializingTestCase<DriverStatus> {
    public void testToXContent() {
        DriverStatus status = new DriverStatus(
            "ABC:123",
            "test",
            "elasticsearch",
            "node-1",
            123413220000L,
            123413243214L,
            123213L,
            55L,
            DriverStatus.Status.RUNNING,
            List.of(
                new OperatorStatus("LuceneSource", LuceneSourceOperatorStatusTests.simple()),
                new OperatorStatus("ValuesSourceReader", ValuesSourceReaderOperatorStatusTests.simple())
            ),
            List.of(new OperatorStatus("ExchangeSink", ExchangeSinkOperatorStatusTests.simple())),
            new DriverSleeps(
                Map.of("driver time", 1L),
                List.of(new DriverSleeps.Sleep("driver time", Thread.currentThread().getName(), 1, 1)),
                List.of(new DriverSleeps.Sleep("driver time", Thread.currentThread().getName(), 1, 1))
            )
        );
        assertThat(Strings.toString(status, true, true), equalTo("""
            {
              "session_id" : "ABC:123",
              "description" : "test",
              "cluster_name" : "elasticsearch",
              "node_name" : "node-1",
              "started" : "1973-11-29T09:27:00.000Z",
              "last_updated" : "1973-11-29T09:27:23.214Z",
              "cpu_nanos" : 123213,
              "cpu_time" : "123.2micros",
              "documents_found" : 222,
              "values_loaded" : 1000,
              "iterations" : 55,
              "status" : "running",
              "completed_operators" : [
                {
                  "operator" : "LuceneSource",
                  "status" :
            """.trim() + " " + LuceneSourceOperatorStatusTests.simpleToJson().replace("\n", "\n      ") + """

                },
                {
                  "operator" : "ValuesSourceReader",
                  "status" :
            """.stripTrailing() + " " + ValuesSourceReaderOperatorStatusTests.simpleToJson().replace("\n", "\n      ") + """

                }
              ],
              "active_operators" : [
                {
                  "operator" : "ExchangeSink",
                  "status" :
            """.stripTrailing() + " " + ExchangeSinkOperatorStatusTests.simpleToJson().replace("\n", "\n      ") + """

                }
              ],
              "sleeps" : {
                "counts" : {
                  "driver time" : 1
                },
                "first" : [
                  {
                    "reason" : "driver time",
                    "thread_name" : "$$THREAD",
                    "sleep" : "1970-01-01T00:00:00.001Z",
                    "sleep_millis" : 1,
                    "wake" : "1970-01-01T00:00:00.001Z",
                    "wake_millis" : 1
                  }
                ],
                "last" : [
                  {
                    "reason" : "driver time",
                    "thread_name" : "$$THREAD",
                    "sleep" : "1970-01-01T00:00:00.001Z",
                    "sleep_millis" : 1,
                    "wake" : "1970-01-01T00:00:00.001Z",
                    "wake_millis" : 1
                  }
                ]
              }
            }""".replace("$$THREAD", Thread.currentThread().getName())));
    }

    @Override
    protected Writeable.Reader<DriverStatus> instanceReader() {
        return DriverStatus::readFrom;
    }

    @Override
    protected DriverStatus createTestInstance() {
        return new DriverStatus(
            randomIdentifier(),
            randomIdentifier(),
            randomIdentifier(),
            randomIdentifier(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomStatus(),
            randomOperatorStatuses(),
            randomOperatorStatuses(),
            DriverSleepsTests.randomDriverSleeps()
        );
    }

    private DriverStatus.Status randomStatus() {
        return randomFrom(DriverStatus.Status.values());
    }

    static List<OperatorStatus> randomOperatorStatuses() {
        return randomList(0, 5, DriverStatusTests::randomOperatorStatus);
    }

    private static OperatorStatus randomOperatorStatus() {
        Supplier<Operator.Status> status = randomFrom(
            new LuceneSourceOperatorStatusTests()::createTestInstance,
            new ValuesSourceReaderOperatorStatusTests()::createTestInstance,
            new ExchangeSinkOperatorStatusTests()::createTestInstance,
            () -> null
        );
        return new OperatorStatus(randomAlphaOfLength(3), status.get());
    }

    @Override
    protected DriverStatus mutateInstance(DriverStatus instance) throws IOException {
        var sessionId = instance.sessionId();
        var description = instance.description();
        var clusterName = instance.clusterName();
        var nodeName = instance.nodeName();
        long started = instance.started();
        long lastUpdated = instance.lastUpdated();
        long cpuNanos = instance.cpuNanos();
        long iterations = instance.iterations();
        var status = instance.status();
        var completedOperators = instance.completedOperators();
        var activeOperators = instance.activeOperators();
        var sleeps = instance.sleeps();
        switch (between(0, 11)) {
            case 0 -> sessionId = randomValueOtherThan(sessionId, ESTestCase::randomIdentifier);
            case 1 -> description = randomValueOtherThan(description, ESTestCase::randomIdentifier);
            case 2 -> clusterName = randomValueOtherThan(clusterName, ESTestCase::randomIdentifier);
            case 3 -> nodeName = randomValueOtherThan(nodeName, ESTestCase::randomIdentifier);
            case 4 -> started = randomValueOtherThan(started, ESTestCase::randomNonNegativeLong);
            case 5 -> lastUpdated = randomValueOtherThan(lastUpdated, ESTestCase::randomNonNegativeLong);
            case 6 -> cpuNanos = randomValueOtherThan(cpuNanos, ESTestCase::randomNonNegativeLong);
            case 7 -> iterations = randomValueOtherThan(iterations, ESTestCase::randomNonNegativeLong);
            case 8 -> status = randomValueOtherThan(status, this::randomStatus);
            case 9 -> completedOperators = randomValueOtherThan(completedOperators, DriverStatusTests::randomOperatorStatuses);
            case 10 -> activeOperators = randomValueOtherThan(activeOperators, DriverStatusTests::randomOperatorStatuses);
            case 11 -> sleeps = randomValueOtherThan(sleeps, DriverSleepsTests::randomDriverSleeps);
            default -> throw new UnsupportedOperationException();
        }
        return new DriverStatus(
            sessionId,
            description,
            clusterName,
            nodeName,
            started,
            lastUpdated,
            cpuNanos,
            iterations,
            status,
            completedOperators,
            activeOperators,
            sleeps
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(LuceneSourceOperator.Status.ENTRY, ValuesSourceReaderOperatorStatus.ENTRY, ExchangeSinkOperator.Status.ENTRY)
        );
    }
}
