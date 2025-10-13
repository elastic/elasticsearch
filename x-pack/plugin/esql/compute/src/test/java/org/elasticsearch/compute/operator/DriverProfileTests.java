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
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DriverProfileTests extends AbstractWireSerializingTestCase<DriverProfile> {
    public void testToXContent() {
        DriverProfile status = new DriverProfile(
            "test",
            "elasticsearch",
            "node-1",
            123413220000L,
            123413243214L,
            10012,
            10000,
            12,
            List.of(
                new OperatorStatus("LuceneSource", LuceneSourceOperatorStatusTests.simple()),
                new OperatorStatus("ValuesSourceReader", ValuesSourceReaderOperatorStatusTests.simple())
            ),
            new DriverSleeps(
                Map.of("driver time", 1L),
                List.of(new DriverSleeps.Sleep("driver time", Thread.currentThread().getName(), 1, 1)),
                List.of(new DriverSleeps.Sleep("driver time", Thread.currentThread().getName(), 1, 1))
            )
        );
        assertThat(Strings.toString(status, true, true), equalTo("""
            {
              "description" : "test",
              "cluster_name" : "elasticsearch",
              "node_name" : "node-1",
              "start" : "1973-11-29T09:27:00.000Z",
              "start_millis" : 123413220000,
              "stop" : "1973-11-29T09:27:23.214Z",
              "stop_millis" : 123413243214,
              "took_nanos" : 10012,
              "took_time" : "10micros",
              "cpu_nanos" : 10000,
              "cpu_time" : "10micros",
              "documents_found" : 222,
              "values_loaded" : 1000,
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
    protected Writeable.Reader<DriverProfile> instanceReader() {
        return DriverProfile::readFrom;
    }

    @Override
    protected DriverProfile createTestInstance() {
        return new DriverProfile(
            randomIdentifier(),
            randomIdentifier(),
            randomIdentifier(),
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
        String shortDescription = instance.description();
        String clusterName = instance.clusterName();
        String nodeName = instance.nodeName();
        long startMillis = instance.startMillis();
        long stopMillis = instance.stopMillis();
        long tookNanos = instance.tookNanos();
        long cpuNanos = instance.cpuNanos();
        long iterations = instance.iterations();
        var operators = instance.operators();
        var sleeps = instance.sleeps();
        switch (between(0, 9)) {
            case 0 -> shortDescription = randomValueOtherThan(shortDescription, DriverStatusTests::randomIdentifier);
            case 1 -> clusterName = randomValueOtherThan(clusterName, DriverStatusTests::randomIdentifier);
            case 2 -> nodeName = randomValueOtherThan(nodeName, DriverStatusTests::randomIdentifier);
            case 3 -> startMillis = randomValueOtherThan(startMillis, ESTestCase::randomNonNegativeLong);
            case 4 -> stopMillis = randomValueOtherThan(startMillis, ESTestCase::randomNonNegativeLong);
            case 5 -> tookNanos = randomValueOtherThan(tookNanos, ESTestCase::randomNonNegativeLong);
            case 6 -> cpuNanos = randomValueOtherThan(cpuNanos, ESTestCase::randomNonNegativeLong);
            case 7 -> iterations = randomValueOtherThan(iterations, ESTestCase::randomNonNegativeLong);
            case 8 -> operators = randomValueOtherThan(operators, DriverStatusTests::randomOperatorStatuses);
            case 9 -> sleeps = randomValueOtherThan(sleeps, DriverSleepsTests::randomDriverSleeps);
            default -> throw new UnsupportedOperationException();
        }
        return new DriverProfile(
            shortDescription,
            clusterName,
            nodeName,
            startMillis,
            stopMillis,
            tookNanos,
            cpuNanos,
            iterations,
            operators,
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
