/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.action.admin.indices.rollover;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public final class RolloverIndexTestHelper {

    // NORELEASE this isn't nice but it's currently the only way to inspect the
    // settings in an update settings request. Need to see if we can make the
    // getter public in ES
    public static void assertRolloverIndexRequest(RolloverRequest request, String alias, Set<Condition<?>> expectedConditions) {
        assertNotNull(request);
        assertEquals(1, request.indices().length);
        assertEquals(alias, request.indices()[0]);
        assertEquals(alias, request.getAlias());
        assertEquals(expectedConditions.size(), request.getConditions().size());
        Set<Object> expectedConditionValues = expectedConditions.stream().map(condition -> condition.value).collect(Collectors.toSet());
        Set<Object> actualConditionValues = request.getConditions().stream().map(condition -> condition.value).collect(Collectors.toSet());
        assertEquals(expectedConditionValues, actualConditionValues);
    }

    // NORELEASE this isn't nice but it's currently the only way to create an
    // UpdateSettingsResponse. Need to see if we can make the constructor public
    // in ES
    public static RolloverResponse createMockResponse(RolloverRequest request, boolean rolledOver) {
        return new RolloverResponse(null, null, Collections.emptySet(), request.isDryRun(), rolledOver, true, true);
    }
}
