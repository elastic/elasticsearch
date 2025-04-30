/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthTracker;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthTracker.DESCRIPTION_LENGTH_LIMIT_KEY;
import static org.elasticsearch.reservedstate.service.FileSettingsServiceTests.NOOP_PUBLISHER;

/**
 * Here, we test {@link FileSettingsHealthTracker} in isolation;
 * we do not test that {@link FileSettingsService} uses it correctly.
 */
public class FileSettingsHealthTrackerTests extends ESTestCase {

    FileSettingsHealthTracker healthTracker;

    @Before
    public void initialize() {
        healthTracker = new FileSettingsHealthTracker(Settings.EMPTY, NOOP_PUBLISHER);
    }

    public void testStartAndStop() {
        assertFalse(healthTracker.getCurrentInfo().isActive());
        healthTracker.startOccurred();
        assertEquals(new FileSettingsHealthInfo(true, 0, 0, null), healthTracker.getCurrentInfo());
        healthTracker.stopOccurred();
        assertFalse(healthTracker.getCurrentInfo().isActive());
    }

    public void testGoodBadGood() {
        healthTracker.startOccurred();
        healthTracker.changeOccurred();
        // This is an unusual state: we've reported a change, but not yet reported whether it passed or failed
        assertEquals(new FileSettingsHealthInfo(true, 1, 0, null), healthTracker.getCurrentInfo());

        healthTracker.failureOccurred("whoopsie 1");
        assertEquals(new FileSettingsHealthInfo(true, 1, 1, "whoopsie 1"), healthTracker.getCurrentInfo());

        healthTracker.changeOccurred();
        healthTracker.failureOccurred("whoopsie 2");
        assertEquals(new FileSettingsHealthInfo(true, 2, 2, "whoopsie 2"), healthTracker.getCurrentInfo());

        healthTracker.changeOccurred();
        healthTracker.successOccurred();
        assertEquals(new FileSettingsHealthInfo(true, 3, 0, null), healthTracker.getCurrentInfo());
    }

    public void testDescriptionIsTruncated() {
        checkTruncatedDescription(9, "123456789", "123456789");
        checkTruncatedDescription(8, "123456789", "1234567…");
        checkTruncatedDescription(1, "12", "…");
    }

    private void checkTruncatedDescription(int lengthLimit, String description, String expectedTruncatedDescription) {
        var tracker = new FileSettingsHealthTracker(
            Settings.builder().put(DESCRIPTION_LENGTH_LIMIT_KEY, lengthLimit).build(),
            NOOP_PUBLISHER
        );
        tracker.startOccurred();
        tracker.changeOccurred();
        tracker.failureOccurred(description);
        assertEquals(new FileSettingsHealthInfo(true, 1, 1, expectedTruncatedDescription), tracker.getCurrentInfo());
    }

}
