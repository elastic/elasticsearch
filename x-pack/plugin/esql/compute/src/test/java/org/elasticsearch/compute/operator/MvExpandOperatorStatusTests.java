/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class MvExpandOperatorStatusTests extends AbstractWireSerializingTestCase<MvExpandOperator.Status> {
    public static MvExpandOperator.Status simple() {
        return new MvExpandOperator.Status(10, 15, 9, 111, 222);
    }

    public static String simpleToJson() {
        return """
            {"pages_received":10,"pages_emitted":15,"noops":9,"rows_received":111,"rows_emitted":222}""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple()), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<MvExpandOperator.Status> instanceReader() {
        return MvExpandOperator.Status::new;
    }

    @Override
    public MvExpandOperator.Status createTestInstance() {
        return new MvExpandOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected MvExpandOperator.Status mutateInstance(MvExpandOperator.Status instance) {
        int pagesReceived = instance.pagesReceived();
        int pagesEmitted = instance.pagesEmitted();
        int noops = instance.noops();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 4)) {
            case 0 -> pagesReceived = randomValueOtherThan(instance.pagesReceived(), ESTestCase::randomNonNegativeInt);
            case 1 -> pagesEmitted = randomValueOtherThan(instance.pagesEmitted(), ESTestCase::randomNonNegativeInt);
            case 2 -> noops = randomValueOtherThan(instance.noops(), ESTestCase::randomNonNegativeInt);
            case 3 -> rowsReceived = randomValueOtherThan(instance.rowsReceived(), ESTestCase::randomNonNegativeLong);
            case 4 -> rowsEmitted = randomValueOtherThan(instance.rowsEmitted(), ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new MvExpandOperator.Status(pagesReceived, pagesEmitted, noops, rowsReceived, rowsEmitted);
    }
}
