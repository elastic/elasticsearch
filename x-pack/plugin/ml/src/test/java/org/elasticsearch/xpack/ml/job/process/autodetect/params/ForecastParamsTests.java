/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ForecastParamsTests extends ESTestCase {

    private static ParseField DURATION = new ParseField("duration");

    public void testForecastIdsAreUnique() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            ids.add(ForecastParams.builder().build().getForecastId());
        }
        assertThat(ids.size(), equalTo(10));
    }

    public void testDurationFormats() {
        assertEquals(34678L,
                ForecastParams.builder().duration(TimeValue.parseTimeValue("34678s", DURATION.getPreferredName())).build().getDuration());
        assertEquals(172800L,
                ForecastParams.builder().duration(TimeValue.parseTimeValue("2d", DURATION.getPreferredName())).build().getDuration());
    }
}
