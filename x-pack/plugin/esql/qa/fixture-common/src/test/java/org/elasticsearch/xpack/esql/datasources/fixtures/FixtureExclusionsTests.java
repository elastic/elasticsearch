/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FixtureExclusionsTests extends ESTestCase {

    public void testEveryExclusionStatesAKindAndAReason() {
        FixtureExclusions exclusions = FixtureExclusions.get();
        assertThat(exclusions.size(), greaterThan(0));
        for (String suite : exclusions.suites()) {
            for (FixtureExclusions.Exclusion e : exclusions.forSuite(suite)) {
                assertThat("kind is parsed", e.kind(), not(nullValue()));
                assertFalse("a reason must not be blank: " + e.caseName(), e.reason().isBlank());
                assertThat("the reason should say something, not just restate the case name", e.reason().length(), greaterThan(20));
            }
        }
    }

    public void testSuitesAreNamedByTheirFormatToken() {
        // A suite name that does not match a token is a typo that would silently exclude nothing.
        Set<String> known = Set.of(
            "csv",
            "tsv",
            "ndjson",
            "orc",
            "parquet",
            "parquet-rs",
            "ndjson-compressed",
            "csv-compressed",
            "tsv-compressed",
            "parquet-compressed"
        );
        for (String suite : FixtureExclusions.get().suites()) {
            assertTrue("unknown suite token [" + suite + "] in the exclusion declaration", known.contains(suite));
        }
    }

    public void testACaseIsNotExcludedFromASuiteThatDoesNotDeclareIt() {
        FixtureExclusions exclusions = FixtureExclusions.get();
        assertThat(exclusions.find("orc", "aCaseNobodyDeclared"), nullValue());
        assertThat(exclusions.casesFor("no-such-suite"), empty());
    }

    public void testDefectsAndFormatLimitsAreDistinguished() {
        FixtureExclusions exclusions = FixtureExclusions.get();
        Set<FixtureExclusions.Kind> seen = new HashSet<>();
        for (String suite : exclusions.suites()) {
            for (FixtureExclusions.Exclusion e : exclusions.forSuite(suite)) {
                seen.add(e.kind());
            }
        }
        // Both kinds must be in use: a table of undifferentiated skips is the problem this replaces.
        assertThat(seen, equalTo(Set.of(FixtureExclusions.Kind.BUG, FixtureExclusions.Kind.RULE)));
    }

    public void testTheSameCaseCanBeExcludedFromMoreThanOneSuiteIndependently() {
        // typeDriftFilterIsStringComparison fails on Java Parquet (pushdown against the pre-widening
        // type) AND on parquet-rs (no reconciliation cast at all) -- different reasons, same case.
        FixtureExclusions exclusions = FixtureExclusions.get();
        FixtureExclusions.Exclusion onParquet = exclusions.find("parquet", "typeDriftFilterIsStringComparison");
        FixtureExclusions.Exclusion onRs = exclusions.find("parquet-rs", "typeDriftFilterIsStringComparison");
        assertThat(onParquet, not(nullValue()));
        assertThat(onRs, not(nullValue()));
        assertThat(onParquet.kind(), equalTo(FixtureExclusions.Kind.BUG));
        assertThat(onRs.kind(), equalTo(FixtureExclusions.Kind.BUG));
    }

    public void testEverySuiteThatDeclaresExclusionsDeclaresAtLeastOne() {
        FixtureExclusions exclusions = FixtureExclusions.get();
        for (String suite : exclusions.suites()) {
            assertThat("suite " + suite + " is listed but excludes nothing", exclusions.casesFor(suite), not(empty()));
        }
    }
}
