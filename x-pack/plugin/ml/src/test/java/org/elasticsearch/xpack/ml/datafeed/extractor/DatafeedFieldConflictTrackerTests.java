/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class DatafeedFieldConflictTrackerTests extends ESTestCase {

    private static final String TIME_FIELD = "@timestamp";

    public void testIdenticalConflictAcrossRechecksProducesEmptyDelta() {
        DatafeedFieldConflictTracker tracker = new DatafeedFieldConflictTracker();
        FieldCapabilitiesResponse response = conflictResponse("status", "keyword", "prod-us", "long", "prod-eu");

        DatafeedFieldConflictTracker.TrackerDelta first = tracker.applyRecheck(response, List.of("status", TIME_FIELD), TIME_FIELD);
        assertThat(first.newOrChangedConflicts(), hasSize(1));

        DatafeedFieldConflictTracker.TrackerDelta second = tracker.applyRecheck(response, List.of("status", TIME_FIELD), TIME_FIELD);
        assertThat(second.isEmpty(), is(true));
    }

    public void testUnlinkThatRemovesOnlyConflictingProjectResolvesConflict() {
        DatafeedFieldConflictTracker tracker = new DatafeedFieldConflictTracker();
        FieldCapabilitiesResponse response = conflictResponse("status", "keyword", "prod-us", "long", "prod-eu");
        tracker.applyRecheck(response, List.of("status"), TIME_FIELD);

        DatafeedFieldConflictTracker.TrackerDelta delta = tracker.handleUnlink(Set.of("prod-eu"));
        assertThat(delta.resolvedFields(), hasSize(1));
        assertThat(delta.resolvedFields().get(0), is("status"));
        assertThat(delta.newOrChangedConflicts(), empty());
    }

    public void testTimeFieldConflictIsTrackedButCompatibleOptionalConflictIsIgnored() {
        DatafeedFieldConflictTracker tracker = new DatafeedFieldConflictTracker();
        FieldCapabilitiesResponse response = new FieldCapabilitiesResponse(
            new String[] { "logs-*" },
            Map.of(
                "status",
                Map.of("long", fieldCaps("status", "long", "prod-us:logs-*"), "integer", fieldCaps("status", "integer", "prod-eu:logs-*")),
                TIME_FIELD,
                Map.of("date", fieldCaps(TIME_FIELD, "date", "prod-us:logs-*"), "long", fieldCaps(TIME_FIELD, "long", "prod-eu:logs-*"))
            )
        );

        DatafeedFieldConflictTracker.TrackerDelta delta = tracker.applyRecheck(response, List.of("status", TIME_FIELD), TIME_FIELD);
        assertThat(delta.newOrChangedConflicts(), hasSize(1));
        assertThat(delta.newOrChangedConflicts().get(0).field(), is(TIME_FIELD));
    }

    private static FieldCapabilitiesResponse conflictResponse(String field, String type1, String project1, String type2, String project2) {
        return new FieldCapabilitiesResponse(
            new String[] { "logs-*" },
            Map.of(
                field,
                Map.of(type1, fieldCaps(field, type1, project1 + ":logs-*"), type2, fieldCaps(field, type2, project2 + ":logs-*"))
            )
        );
    }

    private static FieldCapabilities fieldCaps(String field, String type, String index) {
        return new FieldCapabilities(
            field,
            type,
            false,
            true,
            true,
            null,
            false,
            null,
            new String[] { index },
            null,
            null,
            null,
            null,
            null,
            Map.of()
        );
    }
}
