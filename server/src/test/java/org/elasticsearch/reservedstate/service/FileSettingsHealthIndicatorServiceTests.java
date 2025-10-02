/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.FAILURE_SYMPTOM;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.INACTIVE_SYMPTOM;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.NAME;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.NO_CHANGES_SYMPTOM;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.STALE_SETTINGS_IMPACT;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService.SUCCESS_SYMPTOM;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo.INDETERMINATE;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo.INITIAL_ACTIVE;

/**
 * Tests that {@link FileSettingsHealthIndicatorService} produces the right {@link HealthIndicatorResult}
 * from a given {@link FileSettingsHealthInfo}.
 */
public class FileSettingsHealthIndicatorServiceTests extends ESTestCase {
    FileSettingsHealthIndicatorService healthIndicatorService;

    @Before
    public void initialize() {
        healthIndicatorService = new FileSettingsHealthIndicatorService();
    }

    public void testGreenWhenInactive() {
        var expected = new HealthIndicatorResult(NAME, GREEN, INACTIVE_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
        assertEquals(expected, healthIndicatorService.calculate(INDETERMINATE));
        assertEquals(expected, healthIndicatorService.calculate(new FileSettingsHealthInfo(false, 0, 0, null)));
        assertEquals(
            "Inactive should be GREEN regardless of other fields",
            expected,
            healthIndicatorService.calculate(new FileSettingsHealthInfo(false, 123, 123, "test"))
        );
    }

    public void testNoChangesYet() {
        var expected = new HealthIndicatorResult(NAME, GREEN, NO_CHANGES_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
        assertEquals(expected, healthIndicatorService.calculate(INITIAL_ACTIVE));
        assertEquals(expected, healthIndicatorService.calculate(new FileSettingsHealthInfo(true, 0, 0, null)));
    }

    public void testSuccess() {
        var expected = new HealthIndicatorResult(NAME, GREEN, SUCCESS_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
        assertEquals(expected, healthIndicatorService.calculate(INITIAL_ACTIVE.changed()));
        assertEquals(expected, healthIndicatorService.calculate(INITIAL_ACTIVE.changed().successful()));
        assertEquals(expected, healthIndicatorService.calculate(INITIAL_ACTIVE.changed().failed("whoops").successful()));
    }

    public void testFailure() {
        var info = INITIAL_ACTIVE.changed().failed("whoops");
        assertEquals(yellow(1, "whoops"), healthIndicatorService.calculate(info));
        info = info.failed("whoops again");
        assertEquals(yellow(2, "whoops again"), healthIndicatorService.calculate(info));
        info = info.successful().failed("uh oh");
        assertEquals(yellow(1, "uh oh"), healthIndicatorService.calculate(info));
    }

    private HealthIndicatorResult yellow(long failureStreak, String mostRecentFailure) {
        var details = new SimpleHealthIndicatorDetails(Map.of("failure_streak", failureStreak, "most_recent_failure", mostRecentFailure));
        return new HealthIndicatorResult(NAME, YELLOW, FAILURE_SYMPTOM, details, STALE_SETTINGS_IMPACT, List.of());
    }
}
