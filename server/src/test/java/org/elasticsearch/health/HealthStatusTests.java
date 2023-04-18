/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.stream.Stream;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.UNKNOWN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.hamcrest.Matchers.equalTo;

public class HealthStatusTests extends AbstractWireSerializingTestCase<HealthStatus> {

    public void testAllGreenStatuses() {
        assertEquals(GREEN, HealthStatus.merge(randomStatusesContaining(GREEN)));
    }

    public void testUnknownStatus() {
        assertEquals(UNKNOWN, HealthStatus.merge(randomStatusesContaining(GREEN, UNKNOWN)));
    }

    public void testYellowStatus() {
        assertEquals(YELLOW, HealthStatus.merge(randomStatusesContaining(GREEN, UNKNOWN, YELLOW)));
    }

    public void testRedStatus() {
        assertEquals(RED, HealthStatus.merge(randomStatusesContaining(GREEN, UNKNOWN, YELLOW, RED)));
    }

    public void testEmpty() {
        expectThrows(IllegalArgumentException.class, () -> HealthStatus.merge(Stream.empty()));
    }

    public void testStatusIndicatesHealthProblem() {
        assertFalse(GREEN.indicatesHealthProblem());
        assertFalse(UNKNOWN.indicatesHealthProblem());
        assertTrue(YELLOW.indicatesHealthProblem());
        assertTrue(RED.indicatesHealthProblem());
    }

    private static Stream<HealthStatus> randomStatusesContaining(HealthStatus... statuses) {
        var result = new ArrayList<HealthStatus>();
        for (HealthStatus status : statuses) {
            result.addAll(randomList(1, 10, () -> status));
        }
        return result.stream();
    }

    @Override
    protected Writeable.Reader<HealthStatus> instanceReader() {
        return HealthStatus::read;
    }

    @Override
    protected HealthStatus createTestInstance() {
        return randomFrom(HealthStatus.values());
    }

    @Override
    protected HealthStatus mutateInstance(HealthStatus instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected void assertEqualInstances(HealthStatus expectedInstance, HealthStatus newInstance) {
        assertThat(newInstance, equalTo(expectedInstance));
        assertThat(newInstance.hashCode(), equalTo(expectedInstance.hashCode()));
    }
}
