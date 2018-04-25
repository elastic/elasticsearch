/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */
package org.elasticsearch.action.admin.indices.settings.put;

    import org.elasticsearch.common.settings.Settings;

    import static org.junit.Assert.assertArrayEquals;
    import static org.junit.Assert.assertEquals;
    import static org.junit.Assert.assertNotNull;

public final class UpdateSettingsTestHelper {

    // NORELEASE this isn't nice but it's currently the only way to inspect the
    // settings in an update settings request. Need to see if we can make the
    // getter public in ES
    public static void assertSettingsRequest(UpdateSettingsRequest request, Settings expectedSettings, String... expectedIndices) {
        assertNotNull(request);
        assertArrayEquals(expectedIndices, request.indices());
        assertEquals(expectedSettings, request.settings());
    }

    // NORELEASE this isn't nice but it's currently the only way to create an
    // UpdateSettingsResponse. Need to see if we can make the constructor public
    // in ES
    public static UpdateSettingsResponse createMockResponse(boolean acknowledged) {
        return new UpdateSettingsResponse(acknowledged);
    }
}
