/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.alerts.support.AlertUtils.flattenModel;
import static org.elasticsearch.alerts.support.AlertsDateUtils.formatDate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class AlertUtilsTests {

    @Test
    public void testFlattenModel() throws Exception {
        DateTime now = new DateTime();
        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("a", ImmutableMap.builder().put("a1", new int[] { 0, 1, 2 }).build())
                .put("b", new String[] { "b0", "b1", "b2" })
                .put("c", ImmutableList.of(TimeValue.timeValueSeconds(0), TimeValue.timeValueSeconds(1)))
                .put("d", now)
                .build();

        Map<String, String> result = flattenModel(map);
        assertThat(result.size(), is(9));
        assertThat(result, hasEntry("a.a1.0", "0"));
        assertThat(result, hasEntry("a.a1.1", "1"));
        assertThat(result, hasEntry("a.a1.2", "2"));
        assertThat(result, hasEntry("b.0", "b0"));
        assertThat(result, hasEntry("b.1", "b1"));
        assertThat(result, hasEntry("b.2", "b2"));
        assertThat(result, hasEntry("c.0", "0"));
        assertThat(result, hasEntry("c.1", "1000"));
        assertThat(result, hasEntry("d", formatDate(now)));
    }
}
