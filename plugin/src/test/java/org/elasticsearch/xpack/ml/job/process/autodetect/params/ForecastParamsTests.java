/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.messages.Messages;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ForecastParamsTests extends ESTestCase {

    private static ParseField END = new ParseField("end");
    private static ParseField DURATION = new ParseField("duration");

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

    public void testDurationFormats() {
        assertEquals(34678L,
                ForecastParams.builder().duration(TimeValue.parseTimeValue("34678s", DURATION.getPreferredName())).build().getDuration());
        assertEquals(172800L,
                ForecastParams.builder().duration(TimeValue.parseTimeValue("2d", DURATION.getPreferredName())).build().getDuration());
    }

    public void testDurationEndTimeThrows() {
        ElasticsearchParseException e = ESTestCase.expectThrows(ElasticsearchParseException.class, () -> ForecastParams.builder()
                .endTime("2016-05-01T10:00:00Z", END).duration(TimeValue.parseTimeValue("33d", DURATION.getPreferredName())).build());
        assertEquals(Messages.getMessage(Messages.REST_INVALID_DURATION_AND_ENDTIME), e.getMessage());
    }
}
