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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
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

    /**
     * Every suite an exclusion names must be one the declaration recognises. Reads the declaration rather
     * than restating it: this test previously carried its own five-token list and a second test carried an
     * eight-token one, so three registries disagreed about which suites exist -- the duplicate-registry
     * failure this whole declaration exists to remove, reproduced inside its own tests.
     */
    /**
     * The reason the key carries a spec segment. 24 case names are duplicated across spec files (48
     * instances), so a lookup on the bare name would apply an exclusion declared against one spec to an
     * identically-named case in another -- silently, and looking exactly like a working exclusion.
     */
    public void testAnExclusionDoesNotLeakToASameNamedCaseInAnotherSpec() {
        FixtureExclusions exclusions = FixtureExclusions.get();
        // Declared against external-multifile-temporal for the ndjson suite.
        assertThat(exclusions.find("ndjson", "external-multifile-temporal", "temporalWidensToMinMax"), notNullValue());
        // The csv twin declares an identically-named case; it must NOT be caught by that entry.
        assertThat(exclusions.find("ndjson", "csv-multifile-temporal", "temporalWidensToMinMax"), nullValue());
    }

    public void testSuitesAreNamedByTheirFormatToken() {
        FixtureExclusions exclusions = FixtureExclusions.get();
        for (String suite : exclusions.suites()) {
            assertThat("exclusion names a suite the declaration does not recognise", exclusions.declaredSuites(), hasItem(suite));
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

}
