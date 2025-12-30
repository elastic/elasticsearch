/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class PlanningProfileTests extends AbstractWireSerializingTestCase<PlanningProfile> {

    @Override
    protected Writeable.Reader<PlanningProfile> instanceReader() {
        return PlanningProfile::readFrom;
    }

    @Override
    protected PlanningProfile createTestInstance() {
        return new PlanningProfile(randomTimeSpan(), randomTimeSpan(), randomTimeSpan(), randomTimeSpan(), randomTimeSpan());
    }

    @Override
    protected PlanningProfile mutateInstance(PlanningProfile instance) throws IOException {
        TimeSpan planning = instance.planning().timeSpan();
        TimeSpan parsing = instance.parsing().timeSpan();
        TimeSpan preAnalysis = instance.preAnalysis().timeSpan();
        TimeSpan dependencyResolution = instance.dependencyResolution().timeSpan();
        TimeSpan analysis = instance.analysis().timeSpan();
        switch (randomIntBetween(0, 4)) {
            case 0 -> planning = randomValueOtherThan(planning, PlanningProfileTests::randomTimeSpan);
            case 1 -> parsing = randomValueOtherThan(parsing, PlanningProfileTests::randomTimeSpan);
            case 2 -> preAnalysis = randomValueOtherThan(preAnalysis, PlanningProfileTests::randomTimeSpan);
            case 3 -> dependencyResolution = randomValueOtherThan(dependencyResolution, PlanningProfileTests::randomTimeSpan);
            case 4 -> analysis = randomValueOtherThan(analysis, PlanningProfileTests::randomTimeSpan);
        }
        return new PlanningProfile(planning, parsing, preAnalysis, dependencyResolution, analysis);
    }

    private static TimeSpan randomTimeSpan() {
        return randomBoolean()
            ? new TimeSpan(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
            : null;
    }
}
