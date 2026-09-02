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

public class FallbackPostMapperTests extends ESTestCase {

    public void testMalformedRoutesToIgnoreMalformedInNonColumnarMode() {
        assertThat(
            FallbackPostMapper.route(FallbackPostMapper.Reason.MALFORMED, false),
            is(FallbackPostMapper.Destination.IGNORE_MALFORMED)
        );
    }

    /** In strict-columnar mode, malformed values share the ._on_failure column with multi-value violations. */
    public void testMalformedRoutesToOnFailureInStrictColumnarMode() {
        assertThat(FallbackPostMapper.route(FallbackPostMapper.Reason.MALFORMED, true), is(FallbackPostMapper.Destination.ON_FAILURE));
    }

    public void testMultiValueViolationRoutesToOnFailure() {
        assertThat(
            FallbackPostMapper.route(FallbackPostMapper.Reason.MULTI_VALUE_VIOLATION, randomBoolean()),
            is(FallbackPostMapper.Destination.ON_FAILURE)
        );
    }

    public void testIgnoredSourceReasons() {
        EnumSet<FallbackPostMapper.Reason> ignoredSourceReasons = EnumSet.complementOf(
            EnumSet.of(FallbackPostMapper.Reason.MALFORMED, FallbackPostMapper.Reason.MULTI_VALUE_VIOLATION)
        );
        boolean strictColumnar = randomBoolean();
        for (FallbackPostMapper.Reason reason : ignoredSourceReasons) {
            assertThat(
                "Expected IGNORED_SOURCE for reason " + reason,
                FallbackPostMapper.route(reason, strictColumnar),
                is(FallbackPostMapper.Destination.IGNORED_SOURCE)
            );
        }
    }

    /** Early-out: canAddIgnoredField=false always returns null, regardless of other flags. */
    public void testCannotAddIgnoredFieldReturnsEmpty() {
        var fc = ctx().canAddIgnoredField(false).syntheticFallback(true).build();
        assertNull(FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** Early-out: storesArraysNatively=true always returns null. */
    public void testStoresArraysNativelyReturnsEmpty() {
        var fc = ctx().storesArraysNatively(true).syntheticFallback(true).build();
        assertNull(FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** multi_value=false + synthetic-fallback: pre-capture IS done (commit on success, discard+route on violation). */
    public void testSingleValueIgnoreWithSyntheticFallbackIsPreCaptured() {
        var fc = ctx().syntheticFallback(true).build();
        assertEquals(FallbackPostMapper.Reason.SYNTHETIC_FALLBACK, FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** copy_to destination (not within a copy_to traversal) → COPY_TO_DESTINATION. */
    public void testCopyToDestinationReturnsCopyToReason() {
        var fc = ctx().isCopyToDestinationField(true).isWithinCopyTo(false).build();
        assertEquals(FallbackPostMapper.Reason.COPY_TO_DESTINATION, FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    public void testCopyToWithinCopyToReturnsEmpty() {
        var fc = ctx().isCopyToDestinationField(true).isWithinCopyTo(true).build();
        assertNull(FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** COPY_TO_DESTINATION takes priority over SYNTHETIC_FALLBACK when both conditions match. */
    public void testCopyToDestinationBeatsSyntheticFallback() {
        var fc = ctx().isCopyToDestinationField(true).isWithinCopyTo(false).syntheticFallback(true).build();
        assertEquals(FallbackPostMapper.Reason.COPY_TO_DESTINATION, FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** isWithinCopyTo=true blocks COPY_TO_DESTINATION; SYNTHETIC_FALLBACK wins instead. */
    public void testWithinCopyToFallsBackToSyntheticFallback() {
        var fc = ctx().isCopyToDestinationField(true).isWithinCopyTo(true).syntheticFallback(true).build();
        assertEquals(FallbackPostMapper.Reason.SYNTHETIC_FALLBACK, FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** Mapper in FALLBACK synthetic source mode → SYNTHETIC_FALLBACK. */
    public void testSyntheticFallbackReturnsSyntheticFallbackReason() {
        var fc = ctx().syntheticFallback(true).build();
        assertEquals(FallbackPostMapper.Reason.SYNTHETIC_FALLBACK, FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** source_keep: all → SOURCE_KEEP_ALL. */
    public void testSourceKeepAllReturnsSourceKeepAllReason() {
        var fc = ctx().sourceKeepMode(Mapper.SourceKeepMode.ALL).build();
        assertEquals(FallbackPostMapper.Reason.SOURCE_KEEP_ALL, FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** source_keep: arrays, inside an array, mapper does not parse arrays natively → SOURCE_KEEP_ARRAYS_IN_ARRAY. */
    public void testSourceKeepArraysInArrayScopeReturnsArraysReason() {
        var fc = ctx().sourceKeepMode(Mapper.SourceKeepMode.ARRAYS).inArrayScope(true).parsesArrayValue(false).build();
        assertEquals(FallbackPostMapper.Reason.SOURCE_KEEP_ARRAYS_IN_ARRAY, FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** source_keep: arrays + parsesArrayValue=true → no pre-capture (mapper handles arrays natively). */
    public void testSourceKeepArraysMapperParsesArraysReturnsEmpty() {
        var fc = ctx().sourceKeepMode(Mapper.SourceKeepMode.ARRAYS).inArrayScope(true).parsesArrayValue(true).build();
        assertNull(FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** source_keep: arrays but NOT inside an array scope — the ARRAYS branch is skipped. */
    public void testSourceKeepArraysOutsideArrayScopeReturnsEmpty() {
        var fc = ctx().sourceKeepMode(Mapper.SourceKeepMode.ARRAYS).inArrayScope(false).build();
        assertNull(FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    /** No condition matches → null (no pre-capture needed). */
    public void testNoConditionMatchesReturnsEmpty() {
        var fc = ctx().build();
        assertNull(FallbackPostMapper.resolvePrecaptureReason(fc));
    }

    private static Builder ctx() {
        return new Builder();
    }

    /**
     * Fluent builder for {@link FallbackPostMapper.FieldContext} test fixtures.
     * Defaults represent a field that needs no pre-capture (all conditions false / NONE).
     */
    private static final class Builder {
        private boolean canAddIgnoredField = true;
        private boolean storesArraysNatively = false;
        private boolean syntheticFallback = false;
        private Mapper.SourceKeepMode sourceKeepMode = Mapper.SourceKeepMode.NONE;
        private boolean parsesArrayValue = false;
        private boolean inArrayScope = false;
        private boolean isWithinCopyTo = false;
        private boolean isCopyToDestinationField = false;

        Builder canAddIgnoredField(boolean v) {
            canAddIgnoredField = v;
            return this;
        }

        Builder storesArraysNatively(boolean v) {
            storesArraysNatively = v;
            return this;
        }

        Builder syntheticFallback(boolean v) {
            syntheticFallback = v;
            return this;
        }

        Builder sourceKeepMode(Mapper.SourceKeepMode v) {
            sourceKeepMode = v;
            return this;
        }

        Builder parsesArrayValue(boolean v) {
            parsesArrayValue = v;
            return this;
        }

        Builder inArrayScope(boolean v) {
            inArrayScope = v;
            return this;
        }

        Builder isWithinCopyTo(boolean v) {
            isWithinCopyTo = v;
            return this;
        }

        Builder isCopyToDestinationField(boolean v) {
            isCopyToDestinationField = v;
            return this;
        }

        FallbackPostMapper.FieldContext build() {
            return new FallbackPostMapper.FieldContext(
                canAddIgnoredField,
                storesArraysNatively,
                syntheticFallback,
                sourceKeepMode,
                parsesArrayValue,
                inArrayScope,
                isWithinCopyTo,
                isCopyToDestinationField
            );
        }
    }
}
