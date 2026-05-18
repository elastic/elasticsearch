/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class FilterUnsupportedTemporalitySerializationTests extends AbstractExpressionSerializationTests<FilterUnsupportedTemporality> {

    @Override
    protected FilterUnsupportedTemporality createTestInstance() {
        return new FilterUnsupportedTemporality(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected FilterUnsupportedTemporality mutateInstance(FilterUnsupportedTemporality instance) throws IOException {
        var source = randomSource();
        var histogram = randomValueOtherThan(instance.histogram(), AbstractExpressionSerializationTests::randomChild);
        var temporality = randomValueOtherThan(instance.temporality(), AbstractExpressionSerializationTests::randomChild);
        return new FilterUnsupportedTemporality(source, histogram, temporality);
    }
}
