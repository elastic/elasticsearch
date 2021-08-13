/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.rounding;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTimeZone;

import java.time.ZoneOffset;

import static org.hamcrest.Matchers.is;

public class RoundingDuelTests extends ESTestCase  {

    // dont include nano/micro seconds as rounding would become zero then and throw an exception
    private static final String[] ALLOWED_TIME_SUFFIXES = new String[]{"d", "h", "ms", "s", "m"};

    public void testDuellingImplementations() {
        org.elasticsearch.common.Rounding.DateTimeUnit randomDateTimeUnit =
            randomFrom(org.elasticsearch.common.Rounding.DateTimeUnit.values());
        org.elasticsearch.common.Rounding.Prepared rounding;
        Rounding roundingJoda;

        if (randomBoolean()) {
            rounding = org.elasticsearch.common.Rounding.builder(randomDateTimeUnit).timeZone(ZoneOffset.UTC).build().prepareForUnknown();
            DateTimeUnit dateTimeUnit = DateTimeUnit.resolve(randomDateTimeUnit.getId());
            roundingJoda = Rounding.builder(dateTimeUnit).timeZone(DateTimeZone.UTC).build();
        } else {
            TimeValue interval = timeValue();
            rounding = org.elasticsearch.common.Rounding.builder(interval).timeZone(ZoneOffset.UTC).build().prepareForUnknown();
            roundingJoda = Rounding.builder(interval).timeZone(DateTimeZone.UTC).build();
        }

        long roundValue = randomLong();
        assertThat(roundingJoda.round(roundValue), is(rounding.round(roundValue)));
    }

    static TimeValue timeValue() {
        return TimeValue.parseTimeValue(randomIntBetween(1, 1000) + randomFrom(ALLOWED_TIME_SUFFIXES), "settingName");
    }
}
