/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.regions.Region;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.concurrent.atomic.AtomicBoolean;

public class S3DefaultRegionHolderTests extends ESTestCase {
    public void testSuccess() {
        try (var mockLog = MockLog.capture(S3DefaultRegionHolder.class)) {
            mockLog.addExpectation(new NoLogEventsExpectation());

            final var region = Region.of(randomIdentifier());
            final var regionSupplied = new AtomicBoolean();
            final var regionHolder = new S3DefaultRegionHolder(() -> {
                assertTrue(regionSupplied.compareAndSet(false, true)); // only called once
                return region;
            });

            regionHolder.start();
            assertTrue(regionSupplied.get());
            assertSame(region, regionHolder.getDefaultRegion());
            assertSame(region, regionHolder.getDefaultRegion());

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testFailure() {
        try (var mockLog = MockLog.capture(S3DefaultRegionHolder.class)) {
            final var warningSeenExpectation = new MockLog.EventuallySeenEventExpectation(
                "warning",
                S3DefaultRegionHolder.class.getCanonicalName(),
                Level.WARN,
                "failed to obtain region from default provider chain"
            );
            mockLog.addExpectation(warningSeenExpectation);

            final var regionSupplied = new AtomicBoolean();
            final var regionHolder = new S3DefaultRegionHolder(() -> {
                assertTrue(regionSupplied.compareAndSet(false, true)); // only called once
                throw new ElasticsearchException("simulated");
            });

            regionHolder.start();
            assertTrue(regionSupplied.get());

            warningSeenExpectation.setExpectSeen(); // not seen yet, but will be seen now
            regionHolder.getDefaultRegion();

            mockLog.addExpectation(new NoLogEventsExpectation()); // log message not duplicated
            regionHolder.getDefaultRegion();

            mockLog.assertAllExpectationsMatched();
        }
    }

    private static class NoLogEventsExpectation implements MockLog.LoggingExpectation {
        private boolean seenLogEvent;

        @Override
        public void match(LogEvent event) {
            seenLogEvent = true;
        }

        @Override
        public void assertMatched() {
            assertFalse(seenLogEvent);
        }
    }
}
