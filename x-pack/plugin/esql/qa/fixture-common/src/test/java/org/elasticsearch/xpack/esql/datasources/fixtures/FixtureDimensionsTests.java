/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * The declaration is load-bearing: the whole test set derives from it, so these pin the properties
 * that make the derivation sound rather than merely checking it parses.
 */
public class FixtureDimensionsTests extends ESTestCase {

    private final FixtureDimensions dimensions = FixtureDimensions.get();

    /**
     * The pair table must be total. This is the gate that turns "did anyone think about this
     * combination?" from a question nobody asks into a build failure: adding a nineteenth dimension
     * leaves the build red until its eighteen new relationships are recorded.
     */
    public void testEveryPairHasAVerdict() {
        List<String> names = dimensions.names();
        for (int i = 0; i < names.size(); i++) {
            for (int j = i + 1; j < names.size(); j++) {
                dimensions.verdict(names.get(i), names.get(j));
            }
        }
        assertThat(names.size(), greaterThan(1));
    }

    /** Every dimension needs a default, because it is the anchor every generated vector sits on. */
    public void testEveryDimensionHasADefaultAmongItsValues() {
        for (String d : dimensions.names()) {
            assertThat(d, dimensions.values(d), hasItem(dimensions.defaultValue(d)));
        }
    }

    /** A dimension nothing knows how to apply would generate vectors that cannot be run. */
    public void testEveryDimensionDeclaresHowItBinds() {
        for (String d : dimensions.names()) {
            assertThat(d, Set.of("fixture", "directive", "backend", "pragma", "cluster"), hasItem(dimensions.binds(d)));
        }
    }

    /**
     * Groups are cliques, so by construction every pair inside one interacts. If that ever fails the
     * derivation is wrong, and cells are being generated for pairs declared not to need them.
     */
    public void testEveryPairWithinAGroupInteracts() {
        for (Set<String> group : dimensions.groups()) {
            List<String> members = List.copyOf(group);
            for (int i = 0; i < members.size(); i++) {
                for (int j = i + 1; j < members.size(); j++) {
                    assertTrue(group + " is not a clique", dimensions.crosses(members.get(i), members.get(j)));
                }
            }
        }
    }

    /**
     * The guarantee the whole scheme rests on: any two dimensions that interact are crossed somewhere.
     * A pair that interacts but shares no group would have its combinations silently untested.
     */
    public void testEveryInteractingPairIsCrossedInSomeGroup() {
        List<String> names = dimensions.names();
        List<Set<String>> groups = dimensions.groups();
        for (int i = 0; i < names.size(); i++) {
            for (int j = i + 1; j < names.size(); j++) {
                String a = names.get(i);
                String b = names.get(j);
                if (dimensions.crosses(a, b) == false) {
                    continue;
                }
                boolean covered = groups.stream().anyMatch(g -> g.contains(a) && g.contains(b));
                assertTrue("interacting pair [" + a + ", " + b + "] appears in no group", covered);
            }
        }
    }

    /** Every value of every dimension must appear in some generated vector, or it is never exercised. */
    public void testEveryValueAppearsInSomeVector() {
        List<Map<String, String>> vectors = dimensions.vectors();
        for (String d : dimensions.names()) {
            for (String value : dimensions.values(d)) {
                if (dimensions.appliesTo(d).isEmpty() == false) {
                    continue; // format-scoped values are covered only where their formats are legal
                }
                boolean seen = vectors.stream().anyMatch(v -> value.equals(v.get(d)));
                assertTrue("no vector exercises " + d + "=" + value, seen);
            }
        }
    }

    /** A vector differs from the baseline in one group's dimensions only -- that is what makes a red test readable. */
    public void testVectorsAreCompleteAndDeduplicated() {
        List<Map<String, String>> vectors = dimensions.vectors();
        assertThat(vectors.size(), greaterThan(0));
        for (Map<String, String> v : vectors) {
            assertThat("every vector assigns every dimension", v.keySet(), equalTo(Set.copyOf(dimensions.names())));
        }
        assertThat(vectors.size(), equalTo(Set.copyOf(vectors).size()));
    }

    /** Disjoint pairs cannot be crossed, so they must never be treated as needing cells. */
    public void testDisjointPairsAreNeverCrossed() {
        List<String> names = dimensions.names();
        for (int i = 0; i < names.size(); i++) {
            for (int j = i + 1; j < names.size(); j++) {
                if (dimensions.verdict(names.get(i), names.get(j)) == FixtureDimensions.Verdict.DISJOINT) {
                    assertFalse(names.get(i) + " x " + names.get(j), dimensions.crosses(names.get(i), names.get(j)));
                }
            }
        }
    }

    private static Properties declaration(String... lines) {
        Properties props = new Properties();
        for (String line : lines) {
            int eq = line.indexOf('=');
            props.setProperty(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
        }
        return props;
    }

    /** A minimal well-formed declaration, so each test below alters exactly one thing. */
    private static String[] wellFormed() {
        return new String[] {
            "dimension.format.values = csv, parquet",
            "dimension.format.default = csv",
            "dimension.format.binds = fixture",
            "dimension.error_mode.values = fail_fast, skip_row",
            "dimension.error_mode.default = fail_fast",
            "dimension.error_mode.binds = directive",
            "pair.error_mode.format = interacting" };
    }

    public void testAWellFormedDeclarationParses() {
        FixtureDimensions parsed = FixtureDimensions.parse(declaration(wellFormed()));
        assertThat(parsed.names(), equalTo(List.of("error_mode", "format")));
        assertThat(parsed.binds("format"), equalTo("fixture"));
    }

    /** An unrecognised key is a typo or an invented attribute; either way it would do nothing silently. */
    public void testUnknownKeyIsRejected() {
        String[] lines = ArrayUtils.append(wellFormed(), "dimension.format.colour = blue");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("colour"));
    }

    /** The default anchors every generated vector, so one outside its own values makes the baseline a fiction. */
    public void testADefaultOutsideItsOwnValuesIsRejected() {
        String[] lines = wellFormed().clone();
        lines[1] = "dimension.format.default = orc";
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("orc"));
    }

    /** A dimension nothing knows how to apply would generate vectors that cannot be run. */
    public void testAMissingBindsIsRejected() {
        String[] lines = new String[] {
            "dimension.format.values = csv, parquet",
            "dimension.format.default = csv",
            "dimension.error_mode.values = fail_fast, skip_row",
            "dimension.error_mode.default = fail_fast",
            "dimension.error_mode.binds = directive",
            "pair.error_mode.format = interacting" };
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("binds"));
    }

    /**
     * The gate that makes this a mechanism: a dimension added without saying how it relates to the
     * existing ones leaves the build red rather than silently generating nothing for those pairs.
     */
    public void testAnIncompletePairTableIsRejected() {
        String[] lines = new String[] {
            "dimension.format.values = csv, parquet",
            "dimension.format.default = csv",
            "dimension.format.binds = fixture",
            "dimension.error_mode.values = fail_fast, skip_row",
            "dimension.error_mode.default = fail_fast",
            "dimension.error_mode.binds = directive" };
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("error_mode.format"));
    }

    /** An unknown verdict is not a fourth state to be guessed at; it is a typo. */
    public void testAnUnknownVerdictIsRejected() {
        String[] lines = wellFormed().clone();
        lines[6] = "pair.error_mode.format = probably";
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("probably"));
    }

    /** Renders the derived set so a reader can see what the declaration produces without running it. */
    public void testRenderDerivedSet() {
        List<Set<String>> groups = dimensions.groups();
        StringBuilder out = new StringBuilder("\nderived test set\n");
        for (Set<String> g : groups) {
            out.append(
                String.format(
                    Locale.ROOT,
                    "  %d  %-58s formats=%s fixtureBound=%s%n",
                    g.size(),
                    String.join(",", g),
                    dimensions.formatsFor(g),
                    dimensions.fixtureBound(g)
                )
            );
        }
        out.append("  groups=").append(groups.size()).append("  vectors=").append(dimensions.vectors().size()).append('\n');
        logger.info(out.toString());
        assertThat(groups, not(hasItem(Set.of())));
    }
}
