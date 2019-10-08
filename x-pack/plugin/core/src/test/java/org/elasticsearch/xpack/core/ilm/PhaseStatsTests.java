/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PhaseStats;

import java.io.IOException;
import java.util.Arrays;

public class PhaseStatsTests extends AbstractWireSerializingTestCase<PhaseStats> {

    @Override
    protected PhaseStats createTestInstance() {
        return createRandomInstance();
    }

    public static PhaseStats createRandomInstance() {
        TimeValue after = TimeValue.parseTimeValue(randomTimeValue(), "phase_stats_tests");
        String[] actionNames = randomArray(0, 20, size -> new String[size], () -> randomAlphaOfLengthBetween(1, 20));
        return new PhaseStats(after, actionNames);
    }

    @Override
    protected PhaseStats mutateInstance(PhaseStats instance) throws IOException {
        TimeValue after = instance.getAfter();
        String[] actionNames = instance.getActionNames();
        switch (between(0, 1)) {
        case 0:
            after = randomValueOtherThan(after, () -> TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test"));
            break;
        case 1:
            actionNames = randomValueOtherThanMany(a -> Arrays.equals(a, instance.getActionNames()),
                    () -> randomArray(0, 20, size -> new String[size], () -> randomAlphaOfLengthBetween(1, 20)));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new PhaseStats(after, actionNames);
    }

    @Override
    protected Reader<PhaseStats> instanceReader() {
        return PhaseStats::new;
    }

}
