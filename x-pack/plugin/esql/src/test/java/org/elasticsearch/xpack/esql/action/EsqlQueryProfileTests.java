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
        TimeSpan viewResolution = instance.viewResolution().timeSpan();
        TimeSpan preAnalysis = instance.preAnalysis().timeSpan();
        TimeSpan indicesResolutionMarker = instance.indicesResolutionMarker().timeSpan();
        TimeSpan enrichResolutionMarker = instance.enrichResolutionMarker().timeSpan();
        TimeSpan inferenceResolutionMarker = instance.inferenceResolutionMarker().timeSpan();
        TimeSpan analysis = instance.analysis().timeSpan();
        int fieldCapsCalls = instance.fieldCapsCalls();
        switch (randomIntBetween(0, 9)) {
            case 0 -> query = randomValueOtherThan(query, EsqlQueryProfileTests::randomTimeSpan);
            case 1 -> planning = randomValueOtherThan(planning, EsqlQueryProfileTests::randomTimeSpan);
            case 2 -> parsing = randomValueOtherThan(parsing, EsqlQueryProfileTests::randomTimeSpan);
            case 3 -> viewResolution = randomValueOtherThan(viewResolution, EsqlQueryProfileTests::randomTimeSpan);
            case 4 -> preAnalysis = randomValueOtherThan(preAnalysis, EsqlQueryProfileTests::randomTimeSpan);
            case 5 -> indicesResolutionMarker = randomValueOtherThan(indicesResolutionMarker, EsqlQueryProfileTests::randomTimeSpan);
            case 6 -> enrichResolutionMarker = randomValueOtherThan(enrichResolutionMarker, EsqlQueryProfileTests::randomTimeSpan);
            case 7 -> inferenceResolutionMarker = randomValueOtherThan(inferenceResolutionMarker, EsqlQueryProfileTests::randomTimeSpan);
            case 8 -> analysis = randomValueOtherThan(analysis, EsqlQueryProfileTests::randomTimeSpan);
            case 9 -> fieldCapsCalls = randomValueOtherThan(fieldCapsCalls, () -> randomIntBetween(0, 100));
        }
        return new EsqlQueryProfile(
            query,
            planning,
            parsing,
            viewResolution,
            preAnalysis,
            indicesResolutionMarker,
            enrichResolutionMarker,
            inferenceResolutionMarker,
            analysis,
            fieldCapsCalls
        );
    }

    private static TimeSpan randomTimeSpan() {
        return randomBoolean()
            ? new TimeSpan(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
            : null;
    }
}
