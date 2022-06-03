/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Arrays;
import java.util.List;

public class HealthIndicatorImpactTests extends ESTestCase {
    public void testWriteableSerialization() {
        List<ImpactArea> impactAreas = randomList(1, 3, () -> randomFrom(ImpactArea.values()));
        HealthIndicatorImpact healthIndicatorImpact = new HealthIndicatorImpact(
            randomIntBetween(0, 100),
            randomAlphaOfLength(20),
            impactAreas
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            healthIndicatorImpact,
            impact -> copyWriteable(impact, writableRegistry(), HealthIndicatorImpact::new),
            this::mutateImpact
        );
    }

    private HealthIndicatorImpact mutateImpact(HealthIndicatorImpact originalUserAction) {
        switch (randomIntBetween(1, 3)) {
            case 1 -> {
                List<ImpactArea> unusedImpactAreas = Arrays.stream(ImpactArea.values())
                    .filter(impactArea -> originalUserAction.impactAreas().contains(impactArea) == false)
                    .toList();
                return new HealthIndicatorImpact(originalUserAction.severity(), originalUserAction.impactDescription(), unusedImpactAreas);
            }
            case 2 -> {
                return new HealthIndicatorImpact(
                    originalUserAction.severity() + 1,
                    originalUserAction.impactDescription(),
                    originalUserAction.impactAreas()
                );
            }
            case 3 -> {
                return new HealthIndicatorImpact(originalUserAction.severity(), randomAlphaOfLength(20), originalUserAction.impactAreas());
            }
            default -> throw new IllegalStateException();
        }
    }
}
