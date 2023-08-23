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
        return new MvExpandOperator.Status(10, 9);
    }

    public static String simpleToJson() {
        return """
            {"pages_processed":10,"noops":9}""";
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
        return new MvExpandOperator.Status(randomNonNegativeInt(), randomNonNegativeInt());
    }

    @Override
    protected MvExpandOperator.Status mutateInstance(MvExpandOperator.Status instance) {
        switch (between(0, 1)) {
            case 0:
                return new MvExpandOperator.Status(
                    randomValueOtherThan(instance.pagesProcessed(), ESTestCase::randomNonNegativeInt),
                    instance.noops()
                );
            case 1:
                return new MvExpandOperator.Status(
                    instance.pagesProcessed(),
                    randomValueOtherThan(instance.noops(), ESTestCase::randomNonNegativeInt)
                );
            default:
                throw new UnsupportedOperationException();
        }
    }
}
