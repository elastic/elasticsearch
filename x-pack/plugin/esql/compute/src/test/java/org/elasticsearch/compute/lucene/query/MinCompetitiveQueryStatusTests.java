/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class MinCompetitiveQueryStatusTests extends AbstractWireSerializingTestCase<MinCompetitiveQuery.Status> {
    public static MinCompetitiveQuery.Status simple() {
        return new MinCompetitiveQuery.Status(3, 10, 5, 7, 123456);
    }

    public static String simpleToJson() {
        return """
            {
              "changed_value" : 3,
              "match_all" : 10,
              "match_none" : 5,
              "greater_than_min_competitive" : 7,
              "update_nanos" : 123456,
              "update_time" : "123.4micros"
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<MinCompetitiveQuery.Status> instanceReader() {
        return MinCompetitiveQuery.Status::readFrom;
    }

    public static MinCompetitiveQuery.Status randomMinCompetitiveQueryStatus() {
        return new MinCompetitiveQuery.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong()
        );
    }

    @Override
    public MinCompetitiveQuery.Status createTestInstance() {
        return randomMinCompetitiveQueryStatus();
    }

    @Override
    protected MinCompetitiveQuery.Status mutateInstance(MinCompetitiveQuery.Status instance) {
        int changedValue = instance.changedValue();
        int matchAll = instance.matchAll();
        int matchNone = instance.matchNone();
        int greaterThanMinCompetitive = instance.greaterThanMinCompetitive();
        long updateNanos = instance.updateNanos();
        switch (between(0, 4)) {
            case 0 -> changedValue = randomValueOtherThan(changedValue, ESTestCase::randomNonNegativeInt);
            case 1 -> matchAll = randomValueOtherThan(matchAll, ESTestCase::randomNonNegativeInt);
            case 2 -> matchNone = randomValueOtherThan(matchNone, ESTestCase::randomNonNegativeInt);
            case 3 -> greaterThanMinCompetitive = randomValueOtherThan(greaterThanMinCompetitive, ESTestCase::randomNonNegativeInt);
            case 4 -> updateNanos = randomValueOtherThan(updateNanos, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new MinCompetitiveQuery.Status(changedValue, matchAll, matchNone, greaterThanMinCompetitive, updateNanos);
    }
}
