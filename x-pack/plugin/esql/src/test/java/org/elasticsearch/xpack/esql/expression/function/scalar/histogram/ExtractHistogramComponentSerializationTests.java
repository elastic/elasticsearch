/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.junit.Before;

import java.io.IOException;

public class ExtractHistogramComponentSerializationTests extends AbstractExpressionSerializationTests<ExtractHistogramComponent> {

    @Before
    public void setup() {
        assumeTrue(
            "Only when esql_exponential_histogram feature flag is enabled",
            EsqlCorePlugin.EXPONENTIAL_HISTOGRAM_FEATURE_FLAG.isEnabled()
        );
    }

    @Override
    protected ExtractHistogramComponent createTestInstance() {
        return new ExtractHistogramComponent(randomSource(), randomChild(), randomComponentOrdinal());
    }

    private static Expression randomComponentOrdinal() {
        ExponentialHistogramBlock.Component result = randomFrom(ExponentialHistogramBlock.Component.values());
        return new Literal(randomSource(), result.ordinal(), DataType.INTEGER);
    }

    @Override
    protected ExtractHistogramComponent mutateInstance(ExtractHistogramComponent instance) throws IOException {
        return new ExtractHistogramComponent(
            randomSource(),
            randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild),
            randomValueOtherThan(instance.componentOrdinal(), ExtractHistogramComponentSerializationTests::randomComponentOrdinal)
        );
    }
}
