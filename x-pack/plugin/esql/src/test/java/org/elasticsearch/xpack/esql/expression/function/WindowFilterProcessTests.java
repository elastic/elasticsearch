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

import java.time.ZoneOffset;
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

    private static long minutes(long minutes) {
        return TimeUnit.MINUTES.toMillis(minutes);
    }
}
