/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneSourceOperatorStatusTests;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperatorStatusTests;
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
            123413220000L,
            123413243214L,
            123213L,
            55L,
            DriverStatus.Status.RUNNING,
            List.of(
                new DriverStatus.OperatorStatus("LuceneSource", LuceneSourceOperatorStatusTests.simple()),
                new DriverStatus.OperatorStatus("ValuesSourceReader", ValuesSourceReaderOperatorStatusTests.simple())
            ),
            List.of(new DriverStatus.OperatorStatus("ExchangeSink", ExchangeSinkOperatorStatusTests.simple())),
            new DriverSleeps(
                Map.of("driver time", 1L),
                List.of(new DriverSleeps.Sleep("driver time", 1, 1)),
                List.of(new DriverSleeps.Sleep("driver time", 1, 1))
            )
        );
        assertThat(Strings.toString(status, true, true), equalTo("""
            {
              "sessionId" : "ABC:123",
              "started" : "1973-11-29T09:27:00.000Z",
              "last_updated" : "1973-11-29T09:27:23.214Z",
              "cpu_nanos" : 123213,
              "cpu_time" : "123.2micros",
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
                    "sleep" : "1970-01-01T00:00:00.001Z",
                    "sleep_millis" : 1,
                    "wake" : "1970-01-01T00:00:00.001Z",
                    "wake_millis" : 1
                  }
                ],
                "last" : [
                  {
                    "reason" : "driver time",
                    "sleep" : "1970-01-01T00:00:00.001Z",
                    "sleep_millis" : 1,
                    "wake" : "1970-01-01T00:00:00.001Z",
                    "wake_millis" : 1
                  }
                ]
              }
            }"""));
    }

    @Override
    protected Writeable.Reader<DriverStatus> instanceReader() {
        return DriverStatus::new;
    }

    @Override
    protected DriverStatus createTestInstance() {
        return new DriverStatus(
            randomSessionId(),
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

    private String randomSessionId() {
        return RandomStrings.randomAsciiLettersOfLengthBetween(random(), 1, 15);
    }

    private DriverStatus.Status randomStatus() {
        return randomFrom(DriverStatus.Status.values());
    }

    static List<DriverStatus.OperatorStatus> randomOperatorStatuses() {
        return randomList(0, 5, DriverStatusTests::randomOperatorStatus);
    }

    private static DriverStatus.OperatorStatus randomOperatorStatus() {
        Supplier<Operator.Status> status = randomFrom(
            new LuceneSourceOperatorStatusTests()::createTestInstance,
            new ValuesSourceReaderOperatorStatusTests()::createTestInstance,
            new ExchangeSinkOperatorStatusTests()::createTestInstance,
            () -> null
        );
        return new DriverStatus.OperatorStatus(randomAlphaOfLength(3), status.get());
    }

    @Override
    protected DriverStatus mutateInstance(DriverStatus instance) throws IOException {
        var sessionId = instance.sessionId();
        long started = instance.started();
        long lastUpdated = instance.lastUpdated();
        long cpuNanos = instance.cpuNanos();
        long iterations = instance.iterations();
        var status = instance.status();
        var completedOperators = instance.completedOperators();
        var activeOperators = instance.activeOperators();
        var sleeps = instance.sleeps();
        switch (between(0, 8)) {
            case 0 -> sessionId = randomValueOtherThan(sessionId, this::randomSessionId);
            case 1 -> started = randomValueOtherThan(started, ESTestCase::randomNonNegativeLong);
            case 2 -> lastUpdated = randomValueOtherThan(lastUpdated, ESTestCase::randomNonNegativeLong);
            case 3 -> cpuNanos = randomValueOtherThan(cpuNanos, ESTestCase::randomNonNegativeLong);
            case 4 -> iterations = randomValueOtherThan(iterations, ESTestCase::randomNonNegativeLong);
            case 5 -> status = randomValueOtherThan(status, this::randomStatus);
            case 6 -> completedOperators = randomValueOtherThan(completedOperators, DriverStatusTests::randomOperatorStatuses);
            case 7 -> activeOperators = randomValueOtherThan(activeOperators, DriverStatusTests::randomOperatorStatuses);
            case 8 -> sleeps = randomValueOtherThan(sleeps, DriverSleepsTests::randomDriverSleeps);
            default -> throw new UnsupportedOperationException();
        }
        return new DriverStatus(sessionId, started, lastUpdated, cpuNanos, iterations, status, completedOperators, activeOperators, sleeps);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(LuceneSourceOperator.Status.ENTRY, ValuesSourceReaderOperator.Status.ENTRY, ExchangeSinkOperator.Status.ENTRY)
        );
    }
}
