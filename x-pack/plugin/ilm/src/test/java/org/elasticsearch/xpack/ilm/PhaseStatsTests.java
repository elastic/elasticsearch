/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PhaseStats;

import java.io.IOException;
import java.util.Arrays;

public class PhaseStatsTests extends AbstractWireSerializingTestCase<PhaseStats> {

    @Override
    protected PhaseStats createTestInstance() {
        return randomPhaseStats();
    }

    static PhaseStats randomPhaseStats() {
        TimeValue minimumAge = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
        String[] actionNames = generateRandomStringArray(10, 20, false);
        return new PhaseStats(minimumAge, actionNames);
    }

    @Override
    protected PhaseStats mutateInstance(PhaseStats instance) throws IOException {
        TimeValue minimumAge = instance.getAfter();
        String[] actionNames = instance.getActionNames();
        switch (between(0, 1)) {
        case 0:
            minimumAge = randomValueOtherThan(minimumAge,
                    () -> TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after"));
            break;
        case 1:
            actionNames = Arrays.copyOf(actionNames, actionNames.length + 1);
            actionNames[actionNames.length - 1] = randomAlphaOfLengthBetween(10, 20);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new PhaseStats(minimumAge, actionNames);
    }

    @Override
    protected Reader<PhaseStats> instanceReader() {
        return PhaseStats::new;
    }

}
