/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// Test the phone home/telemetry data
public class MlUsageIT extends ESRestTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @SuppressWarnings("unchecked")
    public void testMLUsage() throws IOException {
        Request request = new Request("GET", "/_xpack/usage");
        var usage = entityAsMap(client().performRequest(request).getEntity());

        var ml = (Map<String, Object>) usage.get("ml");
        assertNotNull(usage.toString(), ml);
        var memoryUsage = (Map<String, Object>) ml.get("memory");
        assertNotNull(ml.toString(), memoryUsage);
        assertThat(memoryUsage.toString(), (Integer) memoryUsage.get("anomaly_detectors_memory_bytes"), greaterThanOrEqualTo(0));
        assertThat(memoryUsage.toString(), (Integer) memoryUsage.get("data_frame_analytics_memory_bytes"), greaterThanOrEqualTo(0));
        assertThat(memoryUsage.toString(), (Integer) memoryUsage.get("pytorch_inference_memory_bytes"), greaterThanOrEqualTo(0));
        assertThat(memoryUsage.toString(), (Integer) memoryUsage.get("total_used_memory_bytes"), greaterThanOrEqualTo(0));
    }

    /**
     * Regression test for the calendars telemetry collector: it must not build a
     * {@code GetCalendarsAction.Request} that sets both a calendar id and paging, which fails
     * validation and silently drops the calendar config-size histogram. When the fetch succeeds
     * the {@code calendars} entry (with its {@code config_sizes}) is present under {@code ml.jobs}.
     */
    @SuppressWarnings("unchecked")
    public void testMLUsageIncludesCalendarConfigSizes() throws IOException {
        Request putCalendar = new Request("PUT", "/_ml/calendars/usage-cal");
        putCalendar.setJsonEntity("""
            { "description": "usage telemetry regression calendar" }
            """);
        client().performRequest(putCalendar);

        Request request = new Request("GET", "/_xpack/usage");
        var usage = entityAsMap(client().performRequest(request).getEntity());

        var ml = (Map<String, Object>) usage.get("ml");
        assertNotNull(usage.toString(), ml);
        var jobsUsage = (Map<String, Object>) ml.get("jobs");
        assertNotNull(ml.toString(), jobsUsage);
        var calendars = (Map<String, Object>) jobsUsage.get("calendars");
        assertNotNull(jobsUsage.toString(), calendars);
        assertNotNull(calendars.toString(), calendars.get("config_sizes"));
    }
}
