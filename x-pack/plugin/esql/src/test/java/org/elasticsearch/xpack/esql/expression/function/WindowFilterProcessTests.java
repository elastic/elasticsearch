/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import com.carrotsearch.hppc.LongLongHashMap;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class WindowFilterProcessTests extends ESTestCase {

    public void testUpperRoundingUsesRoundedValueAsBucketEnd() {
        Rounding.Prepared rounding = Rounding.ToUpperRounding.createRounding(
            Rounding.builder(TimeValue.timeValueMinutes(5)).timeZone(ZoneOffset.UTC).build()
        ).prepareForUnknown();
        LongLongHashMap windowStarts = new LongLongHashMap();

        assertThat(rounding.round(minutes(4)), equalTo(minutes(5)));
        assertThat(WindowFilter.process(minutes(2), rounding, windowStarts, minutes(4)), equalTo(true));
        assertThat(WindowFilter.process(minutes(2), rounding, windowStarts, minutes(3)), equalTo(false));
    }

    public void testEvaluatorSurvivesBucketExpressionRewrite() {
        Configuration configuration = ConfigurationTestUtils.randomConfigurationBuilder()
            .setting(QuerySettings.TIME_ZONE, ZoneOffset.UTC)
            .build();
        Expression window = new Literal(Source.EMPTY, Duration.ofMinutes(1), DataType.TIME_DURATION);
        Expression timestamp = new Literal(Source.EMPTY, minutes(4), DataType.DATETIME);
        Bucket bucket = new Bucket(
            Source.EMPTY,
            timestamp,
            new Literal(Source.EMPTY, Duration.ofMinutes(5), DataType.TIME_DURATION),
            null,
            null,
            null,
            configuration
        );
        WindowFilter filter = new WindowFilter(Source.EMPTY, window, bucket, timestamp);

        // Simulate the optimizer replacing window and timestamp with new expressions.
        // The bucket is not a child of WindowFilter, so it is preserved through replaceChildren.
        WindowFilter transformed = (WindowFilter) filter.replaceChildren(
            List.of(window, new Literal(Source.EMPTY, minutes(4), DataType.DATETIME))
        );

        assertNotNull(transformed.toEvaluator(AbstractFunctionTestCase.toEvaluator()));
    }

    private static long minutes(long minutes) {
        return TimeUnit.MINUTES.toMillis(minutes);
    }
}
