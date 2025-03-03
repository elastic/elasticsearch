/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests for {@link DateDiff} that should not run through the normal testing framework
 */
public class DateDiffFunctionTests extends ESTestCase {

    public void testDateDiffFunctionErrorUnitNotValid() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DateDiff.processMillis(new BytesRef("sseconds"), 0, 0)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Received value [sseconds] is not valid date part to add; "
                    + "did you mean [seconds, second, nanoseconds, milliseconds, microseconds, nanosecond]?"
            )
        );

        e = expectThrows(IllegalArgumentException.class, () -> DateDiff.processMillis(new BytesRef("not-valid-unit"), 0, 0));
        assertThat(
            e.getMessage(),
            containsString(
                "A value of [YEAR, QUARTER, MONTH, DAYOFYEAR, DAY, WEEK, WEEKDAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, "
                    + "NANOSECOND] or their aliases is required; received [not-valid-unit]"
            )
        );
    }

}
