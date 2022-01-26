/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ParseField;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ForecastParamsTests extends ESTestCase {

    private static final ParseField DURATION = new ParseField("duration");

    public void testForecastIdsAreUnique() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            ids.add(ForecastParams.builder().build().getForecastId());
        }
        assertThat(ids.size(), equalTo(10));
    }

    public void testDurationFormats() {
        assertEquals(
            34678L,
            ForecastParams.builder().duration(TimeValue.parseTimeValue("34678s", DURATION.getPreferredName())).build().getDuration()
        );
        assertEquals(
            172800L,
            ForecastParams.builder().duration(TimeValue.parseTimeValue("2d", DURATION.getPreferredName())).build().getDuration()
        );
    }
}
