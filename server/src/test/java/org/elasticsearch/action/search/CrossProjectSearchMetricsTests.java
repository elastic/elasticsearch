/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

public class CrossProjectSearchMetricsTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testToXContentIncludesSearchPhasesWhenPresent() throws IOException {
        CrossProjectSearchMetrics metrics = new CrossProjectSearchMetrics();
        metrics.trackSearchPhaseTookTime("query", 230L);
        metrics.trackSearchPhaseTookTime("fetch", 18L);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        metrics.toXContent(builder, null);
        builder.endObject();

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry(), null, BytesReference.bytes(builder).streamInput())
        ) {
            Map<String, Object> parsed = parser.map();
            Map<String, Object> cpsProfile = (Map<String, Object>) parsed.get(CrossProjectSearchMetrics.CPS_PROFILE_FIELD);
            assertNotNull(cpsProfile);
            Map<String, Object> searchPhases = (Map<String, Object>) cpsProfile.get(CrossProjectSearchMetrics.SEARCH_PHASES_FIELD);
            assertNotNull(searchPhases);
            assertEquals(230, ((Number) searchPhases.get("query")).intValue());
            assertEquals(18, ((Number) searchPhases.get("fetch")).intValue());
        }
    }

    @SuppressWarnings("unchecked")
    public void testToXContentOmitsSearchPhasesWhenEmpty() throws IOException {
        CrossProjectSearchMetrics metrics = new CrossProjectSearchMetrics();
        metrics.trackPlanningPhaseTookTime(25L);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        metrics.toXContent(builder, null);
        builder.endObject();

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry(), null, BytesReference.bytes(builder).streamInput())
        ) {
            Map<String, Object> parsed = parser.map();
            Map<String, Object> cpsProfile = (Map<String, Object>) parsed.get(CrossProjectSearchMetrics.CPS_PROFILE_FIELD);
            assertNotNull(cpsProfile);
            assertNull(cpsProfile.get(CrossProjectSearchMetrics.SEARCH_PHASES_FIELD));
        }
    }

    @SuppressWarnings("unchecked")
    public void testToXContentDfsSearchPhases() throws IOException {
        CrossProjectSearchMetrics metrics = new CrossProjectSearchMetrics();
        metrics.trackSearchPhaseTookTime("dfs", 40L);
        metrics.trackSearchPhaseTookTime("dfs_query", 180L);
        metrics.trackSearchPhaseTookTime("fetch", 20L);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        metrics.toXContent(builder, null);
        builder.endObject();

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry(), null, BytesReference.bytes(builder).streamInput())
        ) {
            Map<String, Object> parsed = parser.map();
            Map<String, Object> cpsProfile = (Map<String, Object>) parsed.get(CrossProjectSearchMetrics.CPS_PROFILE_FIELD);
            assertNotNull(cpsProfile);
            Map<String, Object> searchPhases = (Map<String, Object>) cpsProfile.get(CrossProjectSearchMetrics.SEARCH_PHASES_FIELD);
            assertNotNull(searchPhases);
            assertEquals(40, ((Number) searchPhases.get("dfs")).intValue());
            assertEquals(180, ((Number) searchPhases.get("dfs_query")).intValue());
            assertEquals(20, ((Number) searchPhases.get("fetch")).intValue());
        }
    }

    public void testEqualsAndHashCodeWithSearchPhases() {
        CrossProjectSearchMetrics a = new CrossProjectSearchMetrics();
        a.trackSearchPhaseTookTime("query", 100L);
        a.trackSearchPhaseTookTime("fetch", 50L);

        CrossProjectSearchMetrics b = new CrossProjectSearchMetrics();
        b.trackSearchPhaseTookTime("query", 100L);
        b.trackSearchPhaseTookTime("fetch", 50L);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        CrossProjectSearchMetrics c = new CrossProjectSearchMetrics();
        c.trackSearchPhaseTookTime("query", 999L);

        assertNotEquals(a, c);
    }

    public void testXContentRoundTrip() throws IOException {
        CrossProjectSearchMetrics original = new CrossProjectSearchMetrics();
        original.trackPreProcessingTookTime(5L);
        original.trackPlanningPhaseTookTime(30L);
        original.trackMergingPhaseTookTime(10L);
        original.trackProjectRoundtripTime("proj1", 150L);
        original.trackSearchPhaseTookTime("query", 200L);
        original.trackSearchPhaseTookTime("fetch", 25L);

        // Wrap in an outer object (as in production) so startObject(CPS_PROFILE_FIELD) has a context
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, null);
        builder.endObject();

        // Parse back: navigate past the outer wrapper and the "cps_profile" field name into the inner object
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry(), null, BytesReference.bytes(builder).streamInput())
        ) {
            parser.nextToken(); // START_OBJECT (outer wrapper)
            parser.nextToken(); // FIELD_NAME "cps_profile"
            parser.nextToken(); // START_OBJECT (cps_profile value)
            CrossProjectSearchMetrics parsed = parseCpsMetricsFromJson(parser);

            assertEquals(original.getPlanningPhaseTookTime(), parsed.getPlanningPhaseTookTime());
            assertEquals(original.getMergingPhaseTookTime(), parsed.getMergingPhaseTookTime());
            assertEquals(original.getSearchPhaseTookTimes(), parsed.getSearchPhaseTookTimes());
        }
    }

    /**
     * Re-parses the inner content of a {@code cps_profile} JSON object (parser positioned at START_OBJECT).
     */
    private static CrossProjectSearchMetrics parseCpsMetricsFromJson(XContentParser parser) throws IOException {
        CrossProjectSearchMetrics metrics = new CrossProjectSearchMetrics();
        XContentParser.Token token;
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (CrossProjectSearchMetrics.PLANNING_PHASE_TOOK_TIME_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    metrics.trackPlanningPhaseTookTime(parser.longValue());
                } else if (CrossProjectSearchMetrics.MERGING_PHASE_TOOK_TIME_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    metrics.trackMergingPhaseTookTime(parser.longValue());
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (CrossProjectSearchMetrics.SEARCH_PHASES_FIELD.equals(fieldName)) {
                    String phaseName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            phaseName = parser.currentName();
                        } else if (token.isValue()) {
                            metrics.trackSearchPhaseTookTime(phaseName, parser.longValue());
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return metrics;
    }
}
