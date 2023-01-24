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
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class DriverStatusTests extends AbstractWireSerializingTestCase<DriverStatus> {
    public void testToXContent() {
        DriverStatus status = new DriverStatus(
            DriverStatus.Status.RUNNING,
            List.of(
                new DriverStatus.OperatorStatus("LuceneSource", LuceneSourceOperatorStatusTests.simple()),
                new DriverStatus.OperatorStatus("ValuesSourceReader", ValuesSourceReaderOperatorStatusTests.simple())
            )
        );
        assertThat(
            Strings.toString(status),
            equalTo(
                """
                    {"status":"running","active_operators":[{"operator":"LuceneSource","status":"""
                    + LuceneSourceOperatorStatusTests.simpleToJson()
                    + "},{\"operator\":\"ValuesSourceReader\",\"status\":"
                    + ValuesSourceReaderOperatorStatusTests.simpleToJson()
                    + "}]}"
            )
        );
    }

    @Override
    protected Writeable.Reader<DriverStatus> instanceReader() {
        return DriverStatus::new;
    }

    @Override
    protected DriverStatus createTestInstance() {
        return new DriverStatus(randomStatus(), randomActiveOperators());
    }

    private DriverStatus.Status randomStatus() {
        return randomFrom(DriverStatus.Status.values());
    }

    private List<DriverStatus.OperatorStatus> randomActiveOperators() {
        return randomList(0, 5, this::randomOperatorStatus);
    }

    private DriverStatus.OperatorStatus randomOperatorStatus() {
        Supplier<Operator.Status> status = randomFrom(
            new LuceneSourceOperatorStatusTests()::createTestInstance,
            new ValuesSourceReaderOperatorStatusTests()::createTestInstance,
            () -> null
        );
        return new DriverStatus.OperatorStatus(randomAlphaOfLength(3), status.get());
    }

    @Override
    protected DriverStatus mutateInstance(DriverStatus instance) throws IOException {
        switch (between(0, 1)) {
            case 0:
                return new DriverStatus(randomValueOtherThan(instance.status(), this::randomStatus), instance.activeOperators());
            case 1:
                return new DriverStatus(instance.status(), randomValueOtherThan(instance.activeOperators(), this::randomActiveOperators));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(List.of(LuceneSourceOperator.Status.ENTRY, ValuesSourceReaderOperator.Status.ENTRY));
    }
}
