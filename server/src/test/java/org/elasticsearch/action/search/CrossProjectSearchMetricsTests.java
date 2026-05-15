/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CrossProjectSearchMetricsTests extends ESTestCase {

    public void testToXContentUsesUnknownWhenProjectRoundtripTimeIsNull() throws IOException {
        CrossProjectSearchMetrics metrics = new CrossProjectSearchMetrics();
        metrics.trackProjectRoundtripTime("timed", 42L);
        metrics.getProjectsRoundtripTime().put("unmeasured", null);

        Map<String, Object> root = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(metrics), false);

        @SuppressWarnings("unchecked")
        Map<String, Object> cpsProfile = (Map<String, Object>) root.get(CrossProjectSearchMetrics.CPS_PROFILE_FIELD);
        assertNotNull(cpsProfile);
        @SuppressWarnings("unchecked")
        Map<String, Object> roundTrip = (Map<String, Object>) cpsProfile.get(CrossProjectSearchMetrics.PROJECTS_ROUND_TRIP_TIME);
        assertNotNull(roundTrip);
        @SuppressWarnings("unchecked")
        List<Object> projects = (List<Object>) roundTrip.get(CrossProjectSearchMetrics.PROJECTS_NAME);
        assertNotNull(projects);
        assertEquals(2, projects.size());

        boolean sawTimed = false;
        boolean sawUnmeasured = false;
        for (Object entry : projects) {
            @SuppressWarnings("unchecked")
            Map<String, Object> projectEntry = (Map<String, Object>) entry;
            assertEquals(1, projectEntry.size());
            if (projectEntry.containsKey("timed")) {
                sawTimed = true;
                assertEquals(42, ((Number) projectEntry.get("timed")).intValue());
            } else if (projectEntry.containsKey("unmeasured")) {
                sawUnmeasured = true;
                assertEquals(CrossProjectSearchMetrics.UNKNOWN_PROJECT_ROUND_TRIP_TIME, projectEntry.get("unmeasured"));
            }
        }
        assertTrue(sawTimed);
        assertTrue(sawUnmeasured);
    }
}
