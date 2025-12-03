/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverSleeps;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;

import java.util.List;

public class EsqlQueryResponseProfileTests extends AbstractWireSerializingTestCase<EsqlQueryResponse.Profile> {
    @Override
    protected Writeable.Reader<EsqlQueryResponse.Profile> instanceReader() {
        return EsqlQueryResponse.Profile::readFrom;
    }

    @Override
    protected EsqlQueryResponse.Profile createTestInstance() {
        return new EsqlQueryResponse.Profile(randomDriverProfiles(), randomPlanProfiles(), randomMinimumVersion());
    }

    @Override
    protected EsqlQueryResponse.Profile mutateInstance(EsqlQueryResponse.Profile instance) {
        var drivers = instance.drivers();
        var plans = instance.plans();
        var minimumVersion = instance.minimumVersion();

        switch (between(0, 2)) {
            case 0 -> drivers = randomValueOtherThan(drivers, EsqlQueryResponseProfileTests::randomDriverProfiles);
            case 1 -> plans = randomValueOtherThan(plans, EsqlQueryResponseProfileTests::randomPlanProfiles);
            case 2 -> minimumVersion = randomValueOtherThan(minimumVersion, EsqlQueryResponseProfileTests::randomMinimumVersion);
        }
        return new EsqlQueryResponse.Profile(drivers, plans, minimumVersion);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(List.of(AbstractPageMappingOperator.Status.ENTRY));
    }

    private static List<DriverProfile> randomDriverProfiles() {
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
                randomList(10, EsqlQueryResponseProfileTests::randomOperatorStatus),
                DriverSleeps.empty()
            )
        );
    }

    private static List<PlanProfile> randomPlanProfiles() {
        return randomList(
            10,
            () -> new PlanProfile(
                randomIdentifier(),
                randomIdentifier(),
                randomIdentifier(),
                randomAlphanumericOfLength(1024),
                randomPlanTimeProfile()
            )
        );
    }

    private PlanTimeProfile randomPlanTimeProfile() {
        return randomBoolean() ? null : new PlanTimeProfile(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    private static OperatorStatus randomOperatorStatus() {
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

    public static TransportVersion randomMinimumVersion() {
        return randomBoolean() ? null : EsqlTestUtils.randomMinimumVersion();
    }
}
