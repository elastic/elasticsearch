/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class SnapshotNamePredicateTests extends ESTestCase {

    private static String allSnapshotsSelector() {
        return randomFrom("_all", "_ALL", "_All", "_aLl");
    }

    public void testMatchAll() {
        assertSame(SnapshotNamePredicate.MATCH_ALL, createPredicate(randomBoolean()));
        assertSame(SnapshotNamePredicate.MATCH_ALL, createPredicate(randomBoolean(), "*"));
        assertSame(SnapshotNamePredicate.MATCH_ALL, createPredicate(randomBoolean(), allSnapshotsSelector()));
        assertTrue(SnapshotNamePredicate.MATCH_ALL.test(randomIdentifier(), randomBoolean()));
    }

    private static String currentSnapshotsSelector() {
        return randomFrom("_current", "_CURRENT", "_Current", "_cUrReNt");
    }

    public void testMatchCurrent() {
        assertSame(SnapshotNamePredicate.MATCH_CURRENT_ONLY, createPredicate(randomBoolean(), currentSnapshotsSelector()));
        assertTrue(SnapshotNamePredicate.MATCH_CURRENT_ONLY.test(randomIdentifier(), true));
        assertFalse(SnapshotNamePredicate.MATCH_CURRENT_ONLY.test(randomIdentifier(), false));
    }

    public void testMatchOneNameIgnoreUnavailable() {
        final var requestName = randomIdentifier();
        final var predicates = createPredicate(true, requestName);
        assertTrue(predicates.test(requestName, randomBoolean()));
        assertFalse(predicates.test(randomValueOtherThan(requestName, ESTestCase::randomIdentifier), randomBoolean()));
        assertThat(predicates.requiredNames(), empty());
    }

    public void testMatchOneNameRequireAvailable() {
        final var requestName = randomIdentifier();
        final var predicates = createPredicate(false, requestName);
        assertTrue(predicates.test(requestName, randomBoolean()));
        assertFalse(predicates.test(randomValueOtherThan(requestName, ESTestCase::randomIdentifier), randomBoolean()));
        assertEquals(predicates.requiredNames(), Set.of(requestName));
    }

    public void testMatchWildcard() {
        final var predicates = createPredicate(randomBoolean(), "include-*");
        assertTrue(predicates.test("include-" + randomIdentifier(), randomBoolean()));
        assertFalse(predicates.test("exclude-" + randomIdentifier(), randomBoolean()));
        assertThat(predicates.requiredNames(), empty());
    }

    public void testMatchWildcardAndName() {
        final var requestName = randomIdentifier();
        final var predicates = createPredicate(true, "include-*", requestName);
        assertTrue(predicates.test("include-" + randomIdentifier(), randomBoolean()));
        assertTrue(predicates.test(requestName, randomBoolean()));
        assertFalse(predicates.test("exclude-" + randomIdentifier(), randomBoolean()));
        assertThat(predicates.requiredNames(), empty());

        assertEquals(createPredicate(false, "include-*", requestName).requiredNames(), Set.of(requestName));
    }

    public void testIncludeWildcardExcludeName() {
        final var requestName = randomIdentifier();
        final var predicates = createPredicate(randomBoolean(), "include-*", "-include-" + requestName);
        assertTrue(predicates.test("include-" + randomValueOtherThan(requestName, ESTestCase::randomIdentifier), randomBoolean()));
        assertFalse(predicates.test("include-" + requestName, randomBoolean()));
        assertThat(predicates.requiredNames(), empty());
    }

    public void testIncludeWildcardExcludeWildcard() {
        final var predicates = createPredicate(randomBoolean(), "include-*", "-include-exclude-*");
        assertTrue(predicates.test("include-" + randomIdentifier(), randomBoolean()));
        assertFalse(predicates.test("exclude-" + randomIdentifier(), randomBoolean()));
        assertFalse(predicates.test("include-exclude-" + randomIdentifier(), randomBoolean()));
        assertThat(predicates.requiredNames(), empty());
    }

    public void testIncludeCurrentExcludeWildcard() {
        final var predicates = createPredicate(randomBoolean(), currentSnapshotsSelector(), "-exclude-*");
        assertTrue(predicates.test(randomIdentifier(), true));
        assertFalse(predicates.test("exclude-" + randomIdentifier(), randomBoolean()));
        assertFalse(predicates.test(randomIdentifier(), false));
        assertThat(predicates.requiredNames(), empty());
    }

    public void testIncludeCurrentAndWildcardExcludeName() {
        final var requestName = randomIdentifier();
        final var predicates = createPredicate(randomBoolean(), currentSnapshotsSelector(), "include-*", "-include-" + requestName);
        assertTrue(predicates.test(randomIdentifier(), true));
        assertTrue(predicates.test("include-" + randomValueOtherThan(requestName, ESTestCase::randomIdentifier), false));
        assertFalse(predicates.test("include-" + requestName, randomBoolean()));
        assertThat(predicates.requiredNames(), empty());
    }

    public void testInitialExclude() {
        // NB current behaviour, but could be considered a bug?
        final var requestName = "-" + randomIdentifier();
        final var predicates = createPredicate(false, requestName);
        assertTrue(predicates.test(requestName, randomBoolean()));
        assertThat(predicates.requiredNames(), equalTo(Set.of(requestName)));
    }

    public void testHyphen() {
        // NB current behaviour, but could be considered a bug?
        final var predicates = createPredicate(false, "include-*", "-");
        assertTrue(predicates.test("include-" + randomIdentifier(), randomBoolean()));
        assertTrue(predicates.test("-", randomBoolean()));
        assertThat(predicates.requiredNames(), equalTo(Set.of("-")));
    }

    public void testAllWithExclude() {
        // NB current behaviour, but could be considered a bug?
        final var requestName = randomIdentifier();
        final var predicates = createPredicate(false, "_all", "-" + requestName);
        assertFalse(predicates.test(randomIdentifier(), randomBoolean()));
        assertTrue(predicates.test("_all", randomBoolean()));
        assertTrue(predicates.test("-" + requestName, randomBoolean()));
        assertThat(predicates.requiredNames(), equalTo(Set.of("_all", "-" + requestName)));
    }

    private static SnapshotNamePredicate createPredicate(boolean ignoreUnavailable, String... requestSnapshots) {
        return SnapshotNamePredicate.forSnapshots(ignoreUnavailable, requestSnapshots);
    }
}
