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
        return new MinCompetitiveQuery.Status(2, 0, 1, 3, 5, 450_000L);
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo("""
            {
              "changed_value" : 2,
              "match_all" : 0,
              "match_none" : 1,
              "greater_than_min_competitive" : 3,
              "update_invocations" : 5,
              "update_nanos" : 450000,
              "update_time" : "450micros"
            }"""));
    }

    @Override
    protected Writeable.Reader<MinCompetitiveQuery.Status> instanceReader() {
        return MinCompetitiveQuery.Status::readFrom;
    }

    @Override
    protected MinCompetitiveQuery.Status createTestInstance() {
        return new MinCompetitiveQuery.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected MinCompetitiveQuery.Status mutateInstance(MinCompetitiveQuery.Status instance) {
        return switch (between(0, 5)) {
            case 0 -> new MinCompetitiveQuery.Status(
                randomValueOtherThan(instance.changedValue(), ESTestCase::randomNonNegativeInt),
                instance.matchAll(),
                instance.matchNone(),
                instance.greaterThanMinCompetitive(),
                instance.updateInvocations(),
                instance.updateNanos()
            );
            case 1 -> new MinCompetitiveQuery.Status(
                instance.changedValue(),
                randomValueOtherThan(instance.matchAll(), ESTestCase::randomNonNegativeInt),
                instance.matchNone(),
                instance.greaterThanMinCompetitive(),
                instance.updateInvocations(),
                instance.updateNanos()
            );
            case 2 -> new MinCompetitiveQuery.Status(
                instance.changedValue(),
                instance.matchAll(),
                randomValueOtherThan(instance.matchNone(), ESTestCase::randomNonNegativeInt),
                instance.greaterThanMinCompetitive(),
                instance.updateInvocations(),
                instance.updateNanos()
            );
            case 3 -> new MinCompetitiveQuery.Status(
                instance.changedValue(),
                instance.matchAll(),
                instance.matchNone(),
                randomValueOtherThan(instance.greaterThanMinCompetitive(), ESTestCase::randomNonNegativeInt),
                instance.updateInvocations(),
                instance.updateNanos()
            );
            case 4 -> new MinCompetitiveQuery.Status(
                instance.changedValue(),
                instance.matchAll(),
                instance.matchNone(),
                instance.greaterThanMinCompetitive(),
                randomValueOtherThan(instance.updateInvocations(), ESTestCase::randomNonNegativeInt),
                instance.updateNanos()
            );
            case 5 -> new MinCompetitiveQuery.Status(
                instance.changedValue(),
                instance.matchAll(),
                instance.matchNone(),
                instance.greaterThanMinCompetitive(),
                instance.updateInvocations(),
                randomValueOtherThan(instance.updateNanos(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new UnsupportedOperationException();
        };
    }
}
