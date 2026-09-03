/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Properties;
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
        // Declared against external-multifile-temporal for the orc suite. (This anchored on ndjson until
        // esql-planning#1798 was fixed upstream and those entries were deleted -- the property under test
        // is the lookup's spec scoping, not the particular defect, so it re-anchors rather than retires.)
        assertThat(exclusions.find("orc", "external-multifile-temporal", "temporalWidensToMinMax"), notNullValue());
        // A spec that does not declare this case must NOT be caught by that entry. The name is deliberately
        // one no spec uses: a find() that ignored its spec argument would return the entry above and fail
        // here, which is exactly the leak this pins.
        assertThat(exclusions.find("orc", "csv-multifile-temporal", "temporalWidensToMinMax"), nullValue());
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
        // strictCount is excluded for ndjson AND its compressed twin -- two entries, looked up per suite.
        FixtureExclusions exclusions = FixtureExclusions.get();
        FixtureExclusions.Exclusion onNdjson = exclusions.find("ndjson", "external-multifile-resolution", "strictCount");
        FixtureExclusions.Exclusion onCompressed = exclusions.find("ndjson-compressed", "external-multifile-resolution", "strictCount");
        assertThat(onNdjson, not(nullValue()));
        assertThat(onCompressed, not(nullValue()));
        // And a suite with no entry for it sees nothing, which is what makes the two above independent
        // rather than a single global entry matching everywhere.
        assertThat(exclusions.find("parquet", "external-multifile-resolution", "strictCount"), nullValue());
    }

    /**
     * Kind is a property of the individual entry, not of the case or the suite: a defect owed a fix carries
     * BUG and a permanent constraint carries RULE, and the loader must keep them apart. This anchored on the
     * same case excluded as BUG on parquet and RULE on parquet-rs until parquet-rs was removed upstream
     * (elastic/elasticsearch#157769) and its 45 entries went with it; the two kinds now come from different
     * cases, which pins the same distinction with the data that exists.
     */
    public void testKindDistinguishesADefectFromAPermanentConstraint() {
        // A synthetic declaration, not the live corpus. This test is about the GRAMMAR -- that `bug:` and
        // `rule:` are distinguishable and mean different things -- so naming a real defect as its example
        // coupled it to that defect's lifetime: when #1772 was fixed and its now-stale exclusion deleted,
        // this test went red for a reason that had nothing to do with the grammar it checks.
        Properties props = new Properties();
        props.setProperty("suites", "parquet, ndjson");
        props.setProperty("exclude.parquet.some-spec.aDefectiveCase", "bug: elastic/esql-planning#1 -- the reader is wrong here");
        props.setProperty("exclude.ndjson.some-spec.anImpossibleCase", "rule: the format cannot express this at all");
        FixtureExclusions exclusions = FixtureExclusions.parse(props);

        FixtureExclusions.Exclusion defect = exclusions.find("parquet", "some-spec", "aDefectiveCase");
        FixtureExclusions.Exclusion constraint = exclusions.find("ndjson", "some-spec", "anImpossibleCase");
        assertThat(defect, not(nullValue()));
        assertThat(constraint, not(nullValue()));
        assertThat(defect.kind(), equalTo(FixtureExclusions.Kind.BUG));
        assertThat(constraint.kind(), equalTo(FixtureExclusions.Kind.RULE));
    }

}
