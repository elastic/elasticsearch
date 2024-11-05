/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverSleeps;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.List;
import java.util.stream.Stream;

public class EsqlQueryResponseProfileTests extends AbstractWireSerializingTestCase<EsqlQueryResponse.Profile> {
    @Override
    protected Writeable.Reader<EsqlQueryResponse.Profile> instanceReader() {
        return EsqlQueryResponse.Profile::new;
    }

    @Override
    protected EsqlQueryResponse.Profile createTestInstance() {
        return new EsqlQueryResponse.Profile(randomDriverProfiles());
    }

    @Override
    protected EsqlQueryResponse.Profile mutateInstance(EsqlQueryResponse.Profile instance) {
        return new EsqlQueryResponse.Profile(randomValueOtherThan(instance.drivers(), this::randomDriverProfiles));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Stream.concat(Stream.of(AbstractPageMappingOperator.Status.ENTRY), Block.getNamedWriteables().stream()).toList()
        );
    }

    private List<DriverProfile> randomDriverProfiles() {
        return randomList(10, this::randomDriverProfile);
    }

    private DriverProfile randomDriverProfile() {
        return new DriverProfile(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomList(10, this::randomOperatorStatus),
            DriverSleeps.empty()
        );
    }

    private DriverStatus.OperatorStatus randomOperatorStatus() {
        String name = randomAlphaOfLength(4);
        Operator.Status status = randomBoolean()
            ? null
            : new AbstractPageMappingOperator.Status(randomNonNegativeLong(), between(0, Integer.MAX_VALUE));
        return new DriverStatus.OperatorStatus(name, status);
    }
}
