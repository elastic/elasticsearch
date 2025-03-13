/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.ActionConfigStatsTests;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PhaseStats;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class PhaseStatsTests extends AbstractWireSerializingTestCase<PhaseStats> {

    @Override
    protected PhaseStats createTestInstance() {
        return randomPhaseStats();
    }

    static PhaseStats randomPhaseStats() {
        TimeValue minimumAge = randomTimeValue(0, 1_000_000_000, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS);
        String[] actionNames = generateRandomStringArray(10, 20, false);
        return new PhaseStats(minimumAge, actionNames, ActionConfigStatsTests.createRandomInstance());
    }

    @Override
    protected PhaseStats mutateInstance(PhaseStats instance) {
        TimeValue minimumAge = instance.getAfter();
        String[] actionNames = instance.getActionNames();
        switch (between(0, 1)) {
            case 0 -> minimumAge = randomValueOtherThan(
                minimumAge,
                () -> randomTimeValue(0, 1_000_000_000, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS)
            );
            case 1 -> {
                actionNames = Arrays.copyOf(actionNames, actionNames.length + 1);
                actionNames[actionNames.length - 1] = randomAlphaOfLengthBetween(10, 20);
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PhaseStats(minimumAge, actionNames, instance.getConfigurations());
    }

    @Override
    protected Reader<PhaseStats> instanceReader() {
        return PhaseStats::new;
    }

}
