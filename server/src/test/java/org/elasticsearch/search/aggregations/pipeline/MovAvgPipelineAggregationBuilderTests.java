/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class MovAvgPipelineAggregationBuilderTests extends ESTestCase {

    public void testValidate() {
        String name = randomAlphaOfLength(10);
        MovAvgPipelineAggregationBuilder aggregationBuilder = new MovAvgPipelineAggregationBuilder(name, randomAlphaOfLength(10));
        PipelineAggregationBuilder.ValidationContext validationContext = PipelineAggregationBuilder.ValidationContext.forInsideTree(
            mock(AggregationBuilder.class),
            null
        );
        aggregationBuilder.model(new HoltWintersModel.HoltWintersModelBuilder().period(20).build());
        aggregationBuilder.validate(validationContext);
        List<String> errors = validationContext.getValidationException().validationErrors();
        assertThat(
            errors,
            equalTo(
                Collections.singletonList(
                    "Field [window] must be at least twice as large as the period when using Holt-Winters.  "
                        + "Value provided was [5], which is less than (2*period) == 40"
                )
            )
        );
    }
}
