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
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperatorStatusTests;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DriverProfileTests extends AbstractWireSerializingTestCase<DriverProfile> {
    public void testToXContent() {
        DriverProfile status = new DriverProfile(
            123413220000L,
            123413243214L,
            10012,
            10000,
            12,
            List.of(
                new DriverStatus.OperatorStatus("LuceneSource", LuceneSourceOperatorStatusTests.simple()),
                new DriverStatus.OperatorStatus("ValuesSourceReader", ValuesSourceReaderOperatorStatusTests.simple())
            ),
            new DriverSleeps(
                Map.of("driver time", 1L),
                List.of(new DriverSleeps.Sleep("driver time", 1, 1)),
                List.of(new DriverSleeps.Sleep("driver time", 1, 1))
            )
        );
        assertThat(Strings.toString(status, true, true), equalTo("""
            {
              "start" : "1973-11-29T09:27:00.000Z",
              "start_millis" : 123413220000,
              "stop" : "1973-11-29T09:27:23.214Z",
              "stop_millis" : 123413243214,
              "took_nanos" : 10012,
              "took_time" : "10micros",
              "cpu_nanos" : 10000,
              "cpu_time" : "10micros",
              "iterations" : 12,
              "operators" : [
                {
                  "operator" : "LuceneSource",
                  "status" :
            """.stripTrailing() + " " + LuceneSourceOperatorStatusTests.simpleToJson().replace("\n", "\n      ") + """

                },
                {
                  "operator" : "ValuesSourceReader",
                  "status" :
            """.stripTrailing() + " " + ValuesSourceReaderOperatorStatusTests.simpleToJson().replace("\n", "\n      ") + """

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
    protected Writeable.Reader<DriverProfile> instanceReader() {
        return DriverProfile::new;
    }

    @Override
    protected DriverProfile createTestInstance() {
        return new DriverProfile(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            DriverStatusTests.randomOperatorStatuses(),
            DriverSleepsTests.randomDriverSleeps()
        );
    }

    @Override
    protected DriverProfile mutateInstance(DriverProfile instance) throws IOException {
        long startMillis = instance.startMillis();
        long stopMillis = instance.stopMillis();
        long tookNanos = instance.tookNanos();
        long cpuNanos = instance.cpuNanos();
        long iterations = instance.iterations();
        var operators = instance.operators();
        var sleeps = instance.sleeps();
        switch (between(0, 6)) {
            case 0 -> startMillis = randomValueOtherThan(startMillis, ESTestCase::randomNonNegativeLong);
            case 1 -> stopMillis = randomValueOtherThan(startMillis, ESTestCase::randomNonNegativeLong);
            case 2 -> tookNanos = randomValueOtherThan(tookNanos, ESTestCase::randomNonNegativeLong);
            case 3 -> cpuNanos = randomValueOtherThan(cpuNanos, ESTestCase::randomNonNegativeLong);
            case 4 -> iterations = randomValueOtherThan(iterations, ESTestCase::randomNonNegativeLong);
            case 5 -> operators = randomValueOtherThan(operators, DriverStatusTests::randomOperatorStatuses);
            case 6 -> sleeps = randomValueOtherThan(sleeps, DriverSleepsTests::randomDriverSleeps);
            default -> throw new UnsupportedOperationException();
        }
        return new DriverProfile(startMillis, stopMillis, tookNanos, cpuNanos, iterations, operators, sleeps);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(LuceneSourceOperator.Status.ENTRY, ValuesSourceReaderOperator.Status.ENTRY, ExchangeSinkOperator.Status.ENTRY)
        );
    }
}
