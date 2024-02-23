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

public class AbstractPageMappingOperatorStatusTests extends AbstractWireSerializingTestCase<AbstractPageMappingOperator.Status> {
    public static AbstractPageMappingOperator.Status simple() {
        return new AbstractPageMappingOperator.Status(123);
    }

    public static String simpleToJson() {
        return """
            {"pages_processed":123}""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple()), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<AbstractPageMappingOperator.Status> instanceReader() {
        return AbstractPageMappingOperator.Status::new;
    }

    @Override
    public AbstractPageMappingOperator.Status createTestInstance() {
        return new AbstractPageMappingOperator.Status(randomNonNegativeInt());
    }

    @Override
    protected AbstractPageMappingOperator.Status mutateInstance(AbstractPageMappingOperator.Status instance) {
        return new AbstractPageMappingOperator.Status(randomValueOtherThan(instance.pagesProcessed(), ESTestCase::randomNonNegativeInt));
    }
}
