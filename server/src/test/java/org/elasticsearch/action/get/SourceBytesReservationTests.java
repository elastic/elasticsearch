/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.get;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.action.get.SourceBytesReservation.CHARGE_THRESHOLD_BYTES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SourceBytesReservationTests extends ESTestCase {

    /** Sources are charged once enough has built up, and everything charged is released on close. */
    public void testChargesOnceThresholdIsReachedAndReleasesOnClose() {
        CircuitBreaker breaker = requestBreaker(CHARGE_THRESHOLD_BYTES * 4);
        try (var reservation = new SourceBytesReservation(breaker)) {
            reservation.add(document(CHARGE_THRESHOLD_BYTES / 2));
            assertThat(breaker.getUsed(), equalTo(0L));
            reservation.add(document(CHARGE_THRESHOLD_BYTES / 2));
            assertThat(breaker.getUsed(), equalTo(CHARGE_THRESHOLD_BYTES));
            reservation.add(document(CHARGE_THRESHOLD_BYTES * 2));
            assertThat(breaker.getUsed(), equalTo(CHARGE_THRESHOLD_BYTES * 3));
            reservation.add(document(1));
            assertThat(breaker.getUsed(), equalTo(CHARGE_THRESHOLD_BYTES * 3));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A document the breaker has no room for leaves no charge behind, and documents after it can still be loaded. */
    public void testTripLeavesNoChargeForTheDocument() {
        CircuitBreaker breaker = requestBreaker(CHARGE_THRESHOLD_BYTES * 3);
        try (var reservation = new SourceBytesReservation(breaker)) {
            reservation.add(document(CHARGE_THRESHOLD_BYTES * 2));
            CircuitBreakingException e = expectThrows(
                CircuitBreakingException.class,
                () -> reservation.add(document(CHARGE_THRESHOLD_BYTES * 2))
            );
            assertThat(e.getMessage(), containsString("[" + SourceBytesReservation.BREAKER_LABEL + "]"));
            assertThat(breaker.getUsed(), equalTo(CHARGE_THRESHOLD_BYTES * 2));
            reservation.add(document(CHARGE_THRESHOLD_BYTES));
            assertThat(breaker.getUsed(), equalTo(CHARGE_THRESHOLD_BYTES * 3));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testDocumentWithoutSourceIsNotCharged() {
        CircuitBreaker breaker = requestBreaker(1);
        try (var reservation = new SourceBytesReservation(breaker)) {
            reservation.add(new GetResult("index", "id", 0, 1, 1, true, null, null, null));
            reservation.add(new GetResult("index", "id", -2, 0, -1, false, null, null, null));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private static GetResult document(long sourceBytes) {
        return new GetResult("index", "id", 0, 1, 1, true, new BytesArray(new byte[Math.toIntExact(sourceBytes)]), null, null);
    }

    private static CircuitBreaker requestBreaker(long limitBytes) {
        // without the real-memory parent check, so that the test does not depend on the heap usage of the test JVM
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limitBytes + "b")
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }
}
