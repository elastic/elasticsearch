/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverSleeps;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.List;

public class EsqlQueryResponseProfileTests extends AbstractWireSerializingTestCase<EsqlQueryResponse.Profile> {
    @Override
    protected Writeable.Reader<EsqlQueryResponse.Profile> instanceReader() {
        return EsqlQueryResponse.Profile::readFrom;
    }

    @Override
    protected EsqlQueryResponse.Profile createTestInstance() {
        return new EsqlQueryResponse.Profile(randomDriverProfiles(), randomPlanProfiles());
    }

    @Override
    protected EsqlQueryResponse.Profile mutateInstance(EsqlQueryResponse.Profile instance) {
        return randomBoolean()
            ? new EsqlQueryResponse.Profile(randomValueOtherThan(instance.drivers(), this::randomDriverProfiles), instance.plans())
            : new EsqlQueryResponse.Profile(instance.drivers(), randomValueOtherThan(instance.plans(), this::randomPlanProfiles));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(List.of(AbstractPageMappingOperator.Status.ENTRY));
    }

    private List<DriverProfile> randomDriverProfiles() {
        return randomList(
            10,
            () -> new DriverProfile(
                randomIdentifier(),
                randomIdentifier(),
                randomIdentifier(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomList(10, this::randomOperatorStatus),
                DriverSleeps.empty()
            )
        );
    }

    private List<PlanProfile> randomPlanProfiles() {
        return randomList(
            10,
            () -> new PlanProfile(randomIdentifier(), randomIdentifier(), randomIdentifier(), randomAlphanumericOfLength(1024))
        );
    }

    private OperatorStatus randomOperatorStatus() {
        return new OperatorStatus(
            randomAlphaOfLength(4),
            randomBoolean()
                ? new AbstractPageMappingOperator.Status(
                    randomNonNegativeLong(),
                    randomNonNegativeInt(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
                : null
        );
    }
}
