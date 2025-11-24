/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.junit.Before;

import java.io.IOException;

public class HistogramPercentileSerializationTests extends AbstractExpressionSerializationTests<HistogramPercentile> {

    @Before
    public void setup() {
        assumeTrue(
            "Only when esql_exponential_histogram feature flag is enabled",
            EsqlCorePlugin.EXPONENTIAL_HISTOGRAM_FEATURE_FLAG.isEnabled()
        );
    }

    @Override
    protected HistogramPercentile createTestInstance() {
        return new HistogramPercentile(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected HistogramPercentile mutateInstance(HistogramPercentile instance) throws IOException {
        return new HistogramPercentile(
            randomSource(),
            randomValueOtherThan(instance.histogram(), AbstractExpressionSerializationTests::randomChild),
            randomValueOtherThan(instance.percentile(), AbstractExpressionSerializationTests::randomChild)
        );
    }
}
