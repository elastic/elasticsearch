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
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.DESCRIPTION_LENGTH_LIMIT_KEY;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.FAILURE_SYMPTOM;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.INACTIVE_SYMPTOM;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.NO_CHANGES_SYMPTOM;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.STALE_SETTINGS_IMPACT;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.SUCCESS_SYMPTOM;

/**
 * Here, we test {@link FileSettingsHealthIndicatorService} in isolation;
 * we do not test that {@link FileSettingsService} uses it correctly.
 */
public class FileSettingsHealthIndicatorServiceTests extends ESTestCase {

    FileSettingsHealthIndicatorService healthIndicatorService;

    @Before
    public void initialize() {
        healthIndicatorService = new FileSettingsHealthIndicatorService(Settings.EMPTY);
    }

    public void testInitiallyGreen() {}

    public void testStartAndStop() {
        assertEquals(
            new HealthIndicatorResult("file_settings", GREEN, INACTIVE_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of()),
            healthIndicatorService.calculate(false, null)
        );
        healthIndicatorService.startOccurred();
        assertEquals(
            new HealthIndicatorResult("file_settings", GREEN, NO_CHANGES_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of()),
            healthIndicatorService.calculate(false, null)
        );
        healthIndicatorService.stopOccurred();
        assertEquals(
            new HealthIndicatorResult("file_settings", GREEN, INACTIVE_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of()),
            healthIndicatorService.calculate(false, null)
        );
    }

    public void testGreenYellowYellowGreen() {
        healthIndicatorService.startOccurred();
        healthIndicatorService.changeOccurred();
        // This is a strange case: a change occurred, but neither success nor failure have been reported yet.
        // While the change is still in progress, we don't change the status.
        assertEquals(
            new HealthIndicatorResult("file_settings", GREEN, SUCCESS_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of()),
            healthIndicatorService.calculate(false, null)
        );

        healthIndicatorService.failureOccurred("whoopsie 1");
        assertEquals(
            new HealthIndicatorResult(
                "file_settings",
                YELLOW,
                FAILURE_SYMPTOM,
                new SimpleHealthIndicatorDetails(Map.of("failure_streak", 1L, "most_recent_failure", "whoopsie 1")),
                STALE_SETTINGS_IMPACT,
                List.of()
            ),
            healthIndicatorService.calculate(false, null)
        );

        healthIndicatorService.failureOccurred("whoopsie #2");
        assertEquals(
            new HealthIndicatorResult(
                "file_settings",
                YELLOW,
                FAILURE_SYMPTOM,
                new SimpleHealthIndicatorDetails(Map.of("failure_streak", 2L, "most_recent_failure", "whoopsie #2")),
                STALE_SETTINGS_IMPACT,
                List.of()
            ),
            healthIndicatorService.calculate(false, null)
        );

        healthIndicatorService.successOccurred();
        assertEquals(
            new HealthIndicatorResult("file_settings", GREEN, SUCCESS_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of()),
            healthIndicatorService.calculate(false, null)
        );
    }

    public void testDescriptionIsTruncated() {
        checkTruncatedDescription(9, "123456789", "123456789");
        checkTruncatedDescription(8, "123456789", "1234567…");
        checkTruncatedDescription(1, "12", "…");
    }

    private void checkTruncatedDescription(int lengthLimit, String description, String expectedTruncatedDescription) {
        var service = new FileSettingsHealthIndicatorService(Settings.builder().put(DESCRIPTION_LENGTH_LIMIT_KEY, lengthLimit).build());
        service.startOccurred();
        service.changeOccurred();
        service.failureOccurred(description);
        assertEquals(
            new HealthIndicatorResult(
                "file_settings",
                YELLOW,
                FAILURE_SYMPTOM,
                new SimpleHealthIndicatorDetails(Map.of("failure_streak", 1L, "most_recent_failure", expectedTruncatedDescription)),
                STALE_SETTINGS_IMPACT,
                List.of()
            ),
            service.calculate(false, null)
        );
    }
}
