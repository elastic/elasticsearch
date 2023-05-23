/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PhaseStats;

import java.util.Arrays;

public class PhaseStatsTests extends AbstractWireSerializingTestCase<PhaseStats> {

    @Override
    protected PhaseStats createTestInstance() {
        return createRandomInstance();
    }

    public static PhaseStats createRandomInstance() {
        TimeValue after = TimeValue.parseTimeValue(randomTimeValue(), "phase_stats_tests");
        String[] actionNames = randomArray(0, 20, size -> new String[size], () -> randomAlphaOfLengthBetween(1, 20));
        return new PhaseStats(after, actionNames, ActionConfigStatsTests.createRandomInstance());
    }

    @Override
    protected PhaseStats mutateInstance(PhaseStats instance) {
        TimeValue after = instance.getAfter();
        String[] actionNames = instance.getActionNames();
        switch (between(0, 1)) {
            case 0 -> after = randomValueOtherThan(
                after,
                () -> TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            );
            case 1 -> actionNames = randomValueOtherThanMany(
                a -> Arrays.equals(a, instance.getActionNames()),
                () -> randomArray(0, 20, size -> new String[size], () -> randomAlphaOfLengthBetween(1, 20))
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PhaseStats(after, actionNames, instance.getConfigurations());
    }

    @Override
    protected Reader<PhaseStats> instanceReader() {
        return PhaseStats::new;
    }

}
