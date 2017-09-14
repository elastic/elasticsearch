/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.messages.Messages;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ForecastParamsTests extends ESTestCase {

    private static ParseField END = new ParseField("end");

    public void testDefault_GivesTomorrowTimeInSeconds() {
        long nowSecs = System.currentTimeMillis() / 1000;
        nowSecs += 60 * 60 * 24;

        ForecastParams params = ForecastParams.builder().build();
        assertThat(params.getEndTime(), greaterThanOrEqualTo(nowSecs));
        assertThat(params.getEndTime(), lessThanOrEqualTo(nowSecs +1));
    }

    public void test_UnparseableEndTimeThrows() {
        ElasticsearchParseException e =
                ESTestCase.expectThrows(ElasticsearchParseException.class, () -> ForecastParams.builder().endTime("bad", END).build());
        assertEquals(Messages.getMessage(Messages.REST_INVALID_DATETIME_PARAMS, "end", "bad"), e.getMessage());
    }

    public void testFormats() {
        assertEquals(10L, ForecastParams.builder().endTime("10000", END).build().getEndTime());
        assertEquals(1462096800L, ForecastParams.builder().endTime("2016-05-01T10:00:00Z", END).build().getEndTime());

        long nowSecs = System.currentTimeMillis() / 1000;
        long end = ForecastParams.builder().endTime("now+2H", END).build().getEndTime();
        assertThat(end, greaterThanOrEqualTo(nowSecs + 7200));
        assertThat(end, lessThanOrEqualTo(nowSecs + 7200 +1));
    }
}
