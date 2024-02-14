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

import static org.hamcrest.Matchers.equalTo;

public class DriverProfileTests extends AbstractWireSerializingTestCase<DriverProfile> {
    public void testToXContent() {
        DriverProfile status = new DriverProfile(
            10012,
            10000,
            12,
            List.of(
                new DriverStatus.OperatorStatus("LuceneSource", LuceneSourceOperatorStatusTests.simple()),
                new DriverStatus.OperatorStatus("ValuesSourceReader", ValuesSourceReaderOperatorStatusTests.simple())
            )
        );
        assertThat(Strings.toString(status, true, true), equalTo("""
            {
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
              ]
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
            DriverStatusTests.randomOperatorStatuses()
        );
    }

    @Override
    protected DriverProfile mutateInstance(DriverProfile instance) throws IOException {
        long tookNanos = instance.tookNanos();
        long cpuNanos = instance.cpuNanos();
        long iterations = instance.iterations();
        var operators = instance.operators();
        switch (between(0, 3)) {
            case 0 -> tookNanos = randomValueOtherThan(tookNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> cpuNanos = randomValueOtherThan(cpuNanos, ESTestCase::randomNonNegativeLong);
            case 2 -> iterations = randomValueOtherThan(iterations, ESTestCase::randomNonNegativeLong);
            case 3 -> operators = randomValueOtherThan(operators, DriverStatusTests::randomOperatorStatuses);
            default -> throw new UnsupportedOperationException();
        }
        return new DriverProfile(tookNanos, cpuNanos, iterations, operators);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(LuceneSourceOperator.Status.ENTRY, ValuesSourceReaderOperator.Status.ENTRY, ExchangeSinkOperator.Status.ENTRY)
        );
    }
}
