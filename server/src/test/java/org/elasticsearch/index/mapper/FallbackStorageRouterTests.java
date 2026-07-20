/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;

import static org.hamcrest.Matchers.is;

public class FallbackStorageRouterTests extends ESTestCase {

    // -------------------------------------------------------------------------
    // route() — pure function, exhaustive coverage over every Reason value
    // -------------------------------------------------------------------------

    public void testMalformedRoutesToIgnoreMalformed() {
        assertThat(FallbackStorageRouter.route(FallbackStorageRouter.Reason.MALFORMED), is(FallbackStorageDestination.IGNORE_MALFORMED));
    }

    public void testMultiValueViolationRoutesToOnFailure() {
        assertThat(
            FallbackStorageRouter.route(FallbackStorageRouter.Reason.MULTI_VALUE_VIOLATION),
            is(FallbackStorageDestination.ON_FAILURE)
        );
    }

    public void testIgnoredSourceReasons() {
        EnumSet<FallbackStorageRouter.Reason> ignoredSourceReasons = EnumSet.of(
            FallbackStorageRouter.Reason.SYNTHETIC_FALLBACK,
            FallbackStorageRouter.Reason.SOURCE_KEEP_ALL,
            FallbackStorageRouter.Reason.SOURCE_KEEP_ARRAYS_IN_ARRAY,
            FallbackStorageRouter.Reason.COPY_TO_DESTINATION,
            FallbackStorageRouter.Reason.DYNAMIC_DISABLED,
            FallbackStorageRouter.Reason.DYNAMIC_RUNTIME,
            FallbackStorageRouter.Reason.OBJECT_DISABLED,
            FallbackStorageRouter.Reason.FIELD_LIMIT_EXCEEDED,
            FallbackStorageRouter.Reason.FIELD_NAME_TOO_LONG
        );
        for (FallbackStorageRouter.Reason reason : ignoredSourceReasons) {
            assertThat(
                "Expected IGNORED_SOURCE for reason " + reason,
                FallbackStorageRouter.route(reason),
                is(FallbackStorageDestination.IGNORED_SOURCE)
            );
        }
    }

    /**
     * Every {@link FallbackStorageRouter.Reason} must be handled by {@link FallbackStorageRouter#route}.
     * This test will fail if a new Reason is added without wiring it into {@code route()}.
     */
    public void testAllReasonsAreMapped() {
        for (FallbackStorageRouter.Reason reason : FallbackStorageRouter.Reason.values()) {
            assertNotNull("route() must return a non-null destination for reason " + reason, FallbackStorageRouter.route(reason));
        }
    }

    /**
     * Exactly two destinations are reachable today; the third ({@link FallbackStorageDestination#ON_FAILURE})
     * is only for {@link FallbackStorageRouter.Reason#MULTI_VALUE_VIOLATION}.
     */
    public void testEveryDestinationIsReachable() {
        EnumSet<FallbackStorageDestination> reached = EnumSet.noneOf(FallbackStorageDestination.class);
        for (FallbackStorageRouter.Reason reason : FallbackStorageRouter.Reason.values()) {
            reached.add(FallbackStorageRouter.route(reason));
        }
        assertThat(
            "Every FallbackStorageDestination must be reachable from at least one Reason",
            reached,
            is(EnumSet.allOf(FallbackStorageDestination.class))
        );
    }
}
