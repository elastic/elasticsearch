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

public class EsqlQueryProfileTests extends AbstractWireSerializingTestCase<EsqlQueryProfile> {

    @Override
    protected Writeable.Reader<EsqlQueryProfile> instanceReader() {
        return EsqlQueryProfile::readFrom;
    }

    @Override
    protected EsqlQueryProfile createTestInstance() {
        return new EsqlQueryProfile(
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomIntBetween(0, 100)
        );
    }

    @Override
    protected EsqlQueryProfile mutateInstance(EsqlQueryProfile instance) throws IOException {
        TimeSpan query = instance.total().timeSpan();
        TimeSpan planning = instance.planning().timeSpan();
        TimeSpan parsing = instance.parsing().timeSpan();
        TimeSpan preAnalysis = instance.preAnalysis().timeSpan();
        TimeSpan dependencyResolution = instance.dependencyResolution().timeSpan();
        TimeSpan analysis = instance.analysis().timeSpan();
        int fieldCapsCalls = instance.fieldCapsCalls();
        switch (randomIntBetween(0, 6)) {
            case 0 -> query = randomValueOtherThan(query, EsqlQueryProfileTests::randomTimeSpan);
            case 1 -> planning = randomValueOtherThan(planning, EsqlQueryProfileTests::randomTimeSpan);
            case 2 -> parsing = randomValueOtherThan(parsing, EsqlQueryProfileTests::randomTimeSpan);
            case 3 -> preAnalysis = randomValueOtherThan(preAnalysis, EsqlQueryProfileTests::randomTimeSpan);
            case 4 -> dependencyResolution = randomValueOtherThan(dependencyResolution, EsqlQueryProfileTests::randomTimeSpan);
            case 5 -> analysis = randomValueOtherThan(analysis, EsqlQueryProfileTests::randomTimeSpan);
            case 6 -> fieldCapsCalls = randomValueOtherThan(fieldCapsCalls, () -> randomIntBetween(0, 100));
        }
        return new EsqlQueryProfile(query, planning, parsing, preAnalysis, dependencyResolution, analysis, fieldCapsCalls);
    }

    private static TimeSpan randomTimeSpan() {
        return randomBoolean()
            ? new TimeSpan(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
            : null;
    }
}
