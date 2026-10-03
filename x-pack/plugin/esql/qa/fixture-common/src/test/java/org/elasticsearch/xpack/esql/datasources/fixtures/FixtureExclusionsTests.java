/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
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

    /**
     * Every suite an exclusion names must be one the declaration recognises. Reads the declaration rather
     * than restating it: this test previously carried its own five-token list and a second test carried an
     * eight-token one, so three registries disagreed about which suites exist -- the duplicate-registry
     * failure this whole declaration exists to remove, reproduced inside its own tests.
     */
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

    /**
     * The property #1880 forced into the grammar: a defect reachable only by a value PAIR. Measured on
     * tsvScalarWithNoneParses, escaped mode alone passes and a semicolon delimiter alone passes; only
     * together do they fail. An exclusion naming one value would take away roughly twenty passing cells
     * and report green for it, which is coverage deleted rather than a defect described.
     */
    public void testAPairQualifiedExclusionAppliesOnlyWhenEverySlotMatches() {
        Properties props = new Properties();
        props.setProperty("suites", "tsv-vector");
        props.setProperty(
            "exclude.tsv-vector.some-spec.aCase@data.text_mode.escaped,data.delimiter.semicolon",
            "bug: elastic/esql-planning#1880 -- only the pair splits wrongly"
        );
        FixtureExclusions parsed = FixtureExclusions.parse(props);

        // The vector-aware lookup, because that is the one the suites call: an exclusion is only ever
        // consulted against the vector a case is about to run under.
        assertThat(
            "both slots present",
            excluded(parsed, Map.of("data.text_mode", "escaped", "data.delimiter", "semicolon")),
            equalTo(true)
        );
        assertThat("only the mode", excluded(parsed, Map.of("data.text_mode", "escaped", "data.delimiter", "tab")), equalTo(false));
        assertThat(
            "only the delimiter",
            excluded(parsed, Map.of("data.text_mode", "quoted", "data.delimiter", "semicolon")),
            equalTo(false)
        );
        assertThat("neither", excluded(parsed, Map.of("data.text_mode", "quoted", "data.delimiter", "tab")), equalTo(false));
        assertThat("a slot absent from the vector", excluded(parsed, Map.of("data.text_mode", "escaped")), equalTo(false));
    }

    private static boolean excluded(FixtureExclusions exclusions, Map<String, String> vector) {
        return exclusions.find("tsv-vector", "some-spec", "aCase", vector) != null;
    }

    /** A single slot still means what it always did, and no qualifier still means every vector. */
    public void testASingleSlotAndAnUnqualifiedExclusionAreUnchanged() {
        Properties props = new Properties();
        props.setProperty("suites", "tsv-vector");
        props.setProperty("exclude.tsv-vector.some-spec.oneSlot@data.text_mode.escaped", "bug: elastic/esql-planning#1 -- one value");
        props.setProperty("exclude.tsv-vector.some-spec.everyVector", "rule: the suite cannot express this");
        FixtureExclusions parsed = FixtureExclusions.parse(props);

        assertThat(parsed.find("tsv-vector", "some-spec", "oneSlot", Map.of("data.text_mode", "escaped")), not(nullValue()));
        assertThat(parsed.find("tsv-vector", "some-spec", "oneSlot", Map.of("data.text_mode", "quoted")), nullValue());

        assertThat(parsed.find("tsv-vector", "some-spec", "everyVector", Map.of("data.text_mode", "quoted")), not(nullValue()));
        assertThat(parsed.find("tsv-vector", "some-spec", "everyVector", Map.of()), not(nullValue()));
    }

    public void testAMalformedSlotAnywhereInTheListIsRejected() {
        for (String qualifier : List.of("text_mode.escaped,delimiter", "delimiter,text_mode.escaped", "text_mode.escaped,", "nodot")) {
            Properties props = new Properties();
            props.setProperty("suites", "tsv-vector");
            props.setProperty("exclude.tsv-vector.some-spec.aCase@" + qualifier, "bug: elastic/esql-planning#1 -- malformed");
            Exception e = expectThrows(
                IllegalStateException.class,
                "expected [" + qualifier + "] to be rejected",
                () -> FixtureExclusions.parse(props)
            );
            assertThat(e.getMessage(), containsString("<dimension>.<value>"));
        }
    }

    /**
     * Every qualifier in the REAL declaration names a dimension and a value that exist.
     *
     * <p>The parser checks the qualifier's SHAPE, not its referents, and deliberately so: it takes a
     * synthetic {@code Properties} precisely so the grammar tests do not couple to the live declaration.
     * That leaves a gap the shape check cannot close -- {@code @text_mod.escaped} parses cleanly and then
     * matches no vector ever. A dead {@code bug:} entry is loud, because its case runs and fails; a dead
     * {@code rule:} entry is silent, and silently disables nothing while looking like protection.
     *
     * <p>So the referents are checked here, against the real files, where the coupling belongs.
     */
    public void testEveryQualifierNamesADeclaredDimensionAndValue() {
        FixtureDimensions dimensions = FixtureDimensions.get();
        FixtureExclusions exclusions = FixtureExclusions.get();
        List<String> bad = new ArrayList<>();
        int qualified = 0;
        for (String suite : exclusions.suites()) {
            for (FixtureExclusions.Exclusion e : exclusions.forSuite(suite)) {
                if (e.vectorSlots() == null) {
                    continue;
                }
                qualified++;
                for (String slot : e.vectorSlots().split(",")) {
                    String trimmed = slot.trim();
                    // A dimension name may carry a dot, so the boundary comes from the grammar rather
                    // than from the first separator.
                    String dimension = FixtureDimensions.dimensionNameIn(trimmed);
                    String value = dimension == null ? trimmed : trimmed.substring(dimension.length() + 1);
                    // `rerendered` is derived by the suite rather than declared, so it has no value list --
                    // but it is not therefore unconstrained. The suite computes it with String.valueOf on a
                    // boolean, so exactly two spellings ever reach a vector, and skipping the check let
                    // @rerendered.tru through to match nothing in silence. Checked against the two.
                    if ("rerendered".equals(dimension)) {
                        if (value.equals("true") == false && value.equals("false") == false) {
                            bad.add(suite + "." + e.caseName() + "@" + trimmed + " -- [rerendered] is a boolean");
                        }
                        continue;
                    }
                    if (dimension == null || dimensions.names().contains(dimension) == false) {
                        bad.add(suite + "." + e.caseName() + "@" + trimmed + " -- no such dimension");
                    } else if (dimensions.values(dimension).contains(value) == false) {
                        bad.add(suite + "." + e.caseName() + "@" + trimmed + " -- [" + dimension + "] declares no [" + value + "]");
                    }
                }
            }
        }
        assertThat("this test proves nothing if no exclusion is qualified", qualified, greaterThan(0));
        assertThat("qualifiers naming something undeclared match no vector and protect nothing", bad, empty());
    }

    /** With no suites list nothing can be attributed to a suite, so every entry would apply to nothing. */
    public void testAMissingSuitesListIsRejected() {
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureExclusions.parse(new Properties()));
        assertThat(e.getMessage(), containsString("must declare a 'suites' list"));
    }

    /**
     * An unrecognised key used to be skipped, which made a misspelling read as an absent declaration --
     * the exclusion simply did not happen, and nothing said so.
     */
    public void testAnUnknownKeyIsRejectedRatherThanSkipped() {
        Properties p = declared();
        p.setProperty("reasons.typo", "rule: a misspelt reason prefix");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureExclusions.parse(p));
        assertThat(e.getMessage(), containsString("expected 'suites', 'reason.<name>' or 'exclude.<suite>.<spec>.<case>'"));
    }

    /**
     * The spec segment is required because case names are not unique across spec files: a key without it
     * silences every same-named case, which is a wider exclusion than anyone wrote down.
     */
    public void testAnExclusionKeyMustNameBothSpecAndCase() {
        Properties noDot = declared();
        noDot.setProperty("exclude.csv", "rule: no suite segment");
        assertThat(
            expectThrows(IllegalStateException.class, () -> FixtureExclusions.parse(noDot)).getMessage(),
            containsString("expected exclude.<suite>.<caseName>")
        );

        Properties noSpec = declared();
        noSpec.setProperty("exclude.csv.someCase", "rule: no spec segment");
        assertThat(
            expectThrows(IllegalStateException.class, () -> FixtureExclusions.parse(noSpec)).getMessage(),
            containsString("The spec segment is required")
        );
    }

    /** A suite absent from the declared list is a typo, and its entries would silence nothing. */
    public void testAnExclusionForAnUndeclaredSuiteIsRejected() {
        Properties p = declared();
        p.setProperty("exclude.ghost.some-spec.someCase", "rule: names a suite nobody declared");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureExclusions.parse(p));
        assertThat(e.getMessage(), containsString("phantom suite"));
    }

    /**
     * Every exclusion says whether a fix is owed. A bug is a debt the entry outlives if nobody records it
     * as one; a rule is something the suite cannot express and never will.
     */
    public void testAnExclusionMustCarryATypedReason() {
        Properties noKind = declared();
        noKind.setProperty("exclude.csv.some-spec.someCase", "it just fails");
        assertThat(
            expectThrows(IllegalStateException.class, () -> FixtureExclusions.parse(noKind)).getMessage(),
            containsString("has no kind")
        );

        Properties badKind = declared();
        badKind.setProperty("exclude.csv.some-spec.someCase", "flaky: sometimes red");
        assertThat(
            expectThrows(IllegalStateException.class, () -> FixtureExclusions.parse(badKind)).getMessage(),
            containsString("expected 'bug' or 'rule'")
        );

        Properties noReason = declared();
        noReason.setProperty("exclude.csv.some-spec.someCase", "bug:");
        assertThat(
            expectThrows(IllegalStateException.class, () -> FixtureExclusions.parse(noReason)).getMessage(),
            containsString("states a kind but no reason")
        );
    }

    /** A shared reason that resolves to nothing leaves the entry with no reason at all. */
    public void testAnUnresolvedSharedReasonIsRejected() {
        Properties p = declared();
        p.setProperty("exclude.csv.some-spec.someCase", "bug: @nosuch");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureExclusions.parse(p));
        assertThat(e.getMessage(), containsString("references shared reason [@nosuch]"));
    }

    /** One defect disables several cases, so the paragraph is written once and referenced. */
    public void testASharedReasonIsResolvedForEveryEntryThatReferencesIt() {
        Properties p = declared();
        p.setProperty("reason.shared", "the reader truncates the delimiter, elastic/esql-planning#1");
        p.setProperty("exclude.csv.some-spec.caseOne", "bug: @shared");
        p.setProperty("exclude.csv.some-spec.caseTwo", "bug: @shared");
        FixtureExclusions exclusions = FixtureExclusions.parse(p);
        int seen = 0;
        for (FixtureExclusions.Exclusion e : exclusions.forSuite("csv")) {
            assertThat(e.reason(), containsString("the reader truncates the delimiter"));
            seen++;
        }
        assertThat("both entries resolve the shared paragraph", seen, equalTo(2));
    }

    /** A declaration with a suites list and nothing else. */
    private static Properties declared() {
        Properties p = new Properties();
        p.setProperty("suites", "csv, tsv");
        return p;
    }
}
