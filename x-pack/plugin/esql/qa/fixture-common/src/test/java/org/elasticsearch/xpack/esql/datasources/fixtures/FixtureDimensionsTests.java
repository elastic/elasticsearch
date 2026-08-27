/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
            assertThat(d, FixtureDimensions.BINDS, hasItem(dimensions.binds(d)));
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
            "pair.error_mode.format = interacting",
            // Appended last on purpose: the tests below edit this array by index.
            "dimension.error_mode.key = error_mode" };
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
            "dimension.error_mode.key = error_mode",
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

    /**
     * `binds = directive` says the value travels in the WITH clause, not what it becomes there. Without
     * a key the vector cannot be turned into a query, and the suite would run its default everywhere
     * while reporting the dimension as covered.
     */
    public void testADirectiveDimensionWithoutAKeyOrDerivedIsRejected() {
        String[] lines = wellFormed().clone();
        lines[7] = "dimension.error_mode.applies_to = csv, parquet";
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("neither a key nor derived"));
    }

    /** A key and derived are alternatives; declaring both leaves it ambiguous which one applies. */
    public void testBothAKeyAndDerivedIsRejected() {
        String[] lines = ArrayUtils.append(wellFormed(), "dimension.error_mode.derived = dataset_schema");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("alternatives"));
    }

    /** A fixture-bound dimension with a directive key is a mis-declaration: no injector would read it. */
    public void testANonDirectiveDimensionWithADirectiveKeyIsRejected() {
        String[] lines = ArrayUtils.append(wellFormed(), "dimension.format.key = format");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("binds as [fixture]"));
    }

    /** A value mapping naming a value the dimension does not declare is dead text that never fires. */
    public void testAValueMappingForAnUndeclaredValueIsRejected() {
        String[] lines = ArrayUtils.append(wellFormed(), "dimension.error_mode.value.explode = boom");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("explode"));
    }

    /**
     * Omission IS the default, so a slot at its default must inject nothing -- that is what lets a suite
     * move onto vectors one dimension at a time without changing what it sent before.
     */
    public void testDirectiveSettingsOmitDefaultsAndDerivedSlots() {
        FixtureDimensions d = FixtureDimensions.get();
        Map<String, String> allDefaults = new LinkedHashMap<>();
        for (String name : d.names()) {
            allDefaults.put(name, d.defaultValue(name));
        }
        assertThat(d.directiveSettings(allDefaults), equalTo(Map.of()));

        Map<String, String> varied = new LinkedHashMap<>(allDefaults);
        varied.put("error_mode", "skip_row");
        varied.put("schema_mode", "declared_closed");
        Map<String, String> settings = d.directiveSettings(varied);
        assertThat("a constant slot off its default is injected", settings, equalTo(Map.of("error_mode", "skip_row")));
        assertThat("a derived slot cannot be a constant here", d.derivedFrom("schema_mode"), equalTo("dataset_schema"));
    }

    /** The declaration maps a value to a different spelling only where it says so. */
    public void testAValueMappingIsAppliedAndOthersPassThrough() {
        FixtureDimensions d = FixtureDimensions.get();
        assertThat(d.directiveValue("datetime_format", "custom"), equalTo("strict_date_optional_time"));
        assertThat(d.directiveValue("error_mode", "skip_row"), equalTo("skip_row"));
    }

    /** The baseline has no off-default slot, so it must not render as an empty name. */
    public void testTheAllDefaultsVectorRendersAsDefaults() {
        FixtureDimensions d = FixtureDimensions.get();
        Map<String, String> allDefaults = new LinkedHashMap<>();
        for (String name : d.names()) {
            allDefaults.put(name, d.defaultValue(name));
        }
        assertThat(d.render(allDefaults), equalTo("defaults"));
    }

    /** A rendered name lists only what differs, so a failure names the combination rather than the world. */
    public void testRenderListsOnlyOffDefaultSlots() {
        FixtureDimensions d = FixtureDimensions.get();
        Map<String, String> v = new LinkedHashMap<>();
        for (String name : d.names()) {
            v.put(name, d.defaultValue(name));
        }
        v.put("error_mode", "skip_row");
        assertThat(d.render(v), equalTo("error_mode=skip_row"));
    }

    /**
     * Every vector this returns has to be runnable through the directive seam alone -- that is the whole
     * claim the method makes. A slot bound to anything else would be silently ignored at injection time
     * and the test would report a combination it never actually ran.
     */
    public void testDirectiveExpressibleVectorsVaryOnlyDirectiveBoundSlots() {
        FixtureDimensions d = FixtureDimensions.get();
        List<Map<String, String>> vectors = d.directiveExpressibleVectors("csv");
        assertThat(vectors, not(empty()));
        for (Map<String, String> vector : vectors) {
            assertThat(vector.get("format"), equalTo("csv"));
            for (Map.Entry<String, String> slot : vector.entrySet()) {
                if (slot.getKey().equals("format") || slot.getValue().equals(d.defaultValue(slot.getKey()))) {
                    continue;
                }
                assertThat(
                    "off-default slot [" + slot.getKey() + "] must be expressible as a directive",
                    d.directiveKey(slot.getKey()),
                    notNullValue()
                );
            }
        }
    }

    /** Distinct parameterisations, or the suite would run the same combination twice under two names. */
    public void testDirectiveExpressibleVectorsAreDistinct() {
        FixtureDimensions d = FixtureDimensions.get();
        List<String> names = d.directiveExpressibleVectors("csv").stream().map(d::render).toList();
        assertThat(names.size(), equalTo(Set.copyOf(names).size()));
    }

    /** Every vector must survive the round trip through its own name, or the suite runs the wrong thing. */
    public void testEveryDirectiveExpressibleVectorRoundTripsThroughItsName() {
        FixtureDimensions d = FixtureDimensions.get();
        for (Map<String, String> vector : d.directiveExpressibleVectors("csv")) {
            Map<String, String> back = d.parseRendered(d.render(vector));
            assertThat(d.directiveSettings(back), equalTo(d.directiveSettings(vector)));
        }
    }

    /** A name naming a dimension that no longer exists must fail loudly, not inject nothing. */
    public void testARenderedNameWithAnUnknownDimensionIsRejected() {
        FixtureDimensions d = FixtureDimensions.get();
        Exception e = expectThrows(IllegalArgumentException.class, () -> d.parseRendered("nonesuch=x"));
        assertThat(e.getMessage(), containsString("nonesuch"));
    }

    /** Likewise a value the dimension has since dropped. */
    public void testARenderedNameWithAnUndeclaredValueIsRejected() {
        FixtureDimensions d = FixtureDimensions.get();
        Exception e = expectThrows(IllegalArgumentException.class, () -> d.parseRendered("error_mode=explode"));
        assertThat(e.getMessage(), containsString("explode"));
    }

    /**
     * A value needing a companion setting cannot be injected alone: the dataset registration is rejected
     * outright. Generating such a vector produces a red test that says nothing about the product, so the
     * selection has to drop it -- and the declaration has to be why, not a hard-coded name here.
     */
    public void testDirectiveExpressibleVectorsExcludeValuesThatNeedACompanion() {
        FixtureDimensions d = FixtureDimensions.get();
        for (Map<String, String> vector : d.directiveExpressibleVectors("csv")) {
            for (Map.Entry<String, String> slot : vector.entrySet()) {
                if (slot.getValue().equals(d.defaultValue(slot.getKey()))) {
                    continue;
                }
                assertThat(
                    "slot [" + slot.getKey() + "=" + slot.getValue() + "] needs a companion and cannot stand alone",
                    d.derivedFromForValue(slot.getKey(), slot.getValue()),
                    nullValue()
                );
            }
        }
    }

    /** The specific case the generated suite found: template detection needs the path template with it. */
    public void testTemplatePartitionDetectionIsDeclaredAsNeedingAPath() {
        FixtureDimensions d = FixtureDimensions.get();
        assertThat(d.derivedFromForValue("partition_detection", "template"), equalTo("partition_path"));
        assertThat("the other values stand alone", d.derivedFromForValue("partition_detection", "hive"), nullValue());
    }

    /**
     * F1, and it is load-bearing. A pragma is not a dataset setting. If its key reached the directive map
     * the dimension would look directive-expressible, the per-format vector counts would move, and every
     * dataset WITH clause would carry an unknown key -- breaking suites that pass today.
     */
    public void testAPragmaKeyIsNotADirectiveSetting() {
        FixtureDimensions d = FixtureDimensions.get();
        assertThat("distribution binds as a pragma", d.binds("distribution"), equalTo("pragma"));
        assertThat(d.pragmaKey("distribution"), equalTo("external_distribution"));
        assertThat("and must NOT be reachable as a directive", d.directiveKey("distribution"), nullValue());

        Map<String, String> varied = new LinkedHashMap<>();
        for (String name : d.names()) {
            varied.put(name, d.defaultValue(name));
        }
        varied.put("distribution", "round_robin");
        assertThat("a pragma slot contributes no WITH settings", d.directiveSettings(varied), equalTo(Map.of()));
    }

    /**
     * The counts the live vector suites are built on. A silent move here changes what CI runs.
     *
     * <p>tsv is 9 rather than 18 because the baseline is filled with the format's OWN default. The
     * reader reads .tsv as plain, so a vector carrying quoted is off-default there and needs a
     * quoted-tsv fixture that nothing writes. While the baseline was filled globally those nine vectors
     * were selected anyway and ran with no mode injected -- asserting quoted while the reader used
     * plain. They were not testing what their name said.
     */
    public void testDirectiveExpressibleCountsPerFormat() {
        FixtureDimensions d = FixtureDimensions.get();
        assertThat(d.directiveExpressibleVectors("csv").size(), equalTo(24));
        assertThat(d.directiveExpressibleVectors("tsv").size(), equalTo(9));
        assertThat(d.directiveExpressibleVectors("ndjson").size(), equalTo(18));
        assertThat(d.directiveExpressibleVectors("parquet").size(), equalTo(9));
    }

    /**
     * The universe is a property of the contract, not of any format's defaults. Correcting the baseline
     * fill must not add or drop a single vector -- if it does, the per-format default has leaked into
     * the derivation rather than only into what is selectable from it.
     */
    public void testTheVectorUniverseIsUnchangedByPerFormatDefaults() {
        int[] seen = { 0 };
        FixtureDimensions.get().forEachVector(v -> seen[0]++);
        assertThat(seen[0], equalTo(11685));
    }

    /** The reader default is per-extension: a .tsv is read plain where a .csv is read quoted. */
    public void testAPerFormatDefaultOverridesTheBaseDefault() {
        FixtureDimensions d = FixtureDimensions.get();
        assertThat(d.defaultValue("text_mode"), equalTo("quoted"));
        assertThat(d.defaultValue("text_mode", "tsv"), equalTo("plain"));
        assertThat("a format with no override falls back", d.defaultValue("text_mode", "csv"), equalTo("quoted"));
    }

    /** The scheme-to-backend correspondence is declared, so renaming a backend cannot rot it silently. */
    public void testTheSchemeToBackendCorrespondenceIsDeclaredAndTotal() {
        FixtureDimensions d = FixtureDimensions.get();
        assertThat(d.backendFor("storage_scheme", "wasbs"), equalTo("AZURE"));
        assertThat(d.backendFor("storage_scheme", "file"), equalTo("LOCAL"));
        for (String scheme : d.values("storage_scheme")) {
            assertThat("every scheme names a backend", d.backendFor("storage_scheme", scheme), notNullValue());
        }
    }

    /** A read key on a binding nothing announces would be wiring that only looks like it exists. */
    public void testAReadKeyOnANonFixtureDimensionIsRejected() {
        String[] lines = ArrayUtils.append(wellFormed(), "dimension.error_mode.read_key = mode");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("read_key"));
    }

    /** An unknown binding is a typo that would otherwise silently exclude the dimension from every seam. */
    public void testAnUnknownBindsIsRejected() {
        String[] lines = wellFormed().clone();
        lines[2] = "dimension.format.binds = magic";
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("magic"));
    }

    /** A per-format default naming a format that does not exist would never apply to anything. */
    public void testAPerFormatDefaultForAnUndeclaredFormatIsRejected() {
        String[] lines = ArrayUtils.append(wellFormed(), "dimension.error_mode.default.orc = skip_row");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("undeclared format"));
    }

    /** A partial backend map is a forgotten line wearing a decision's face. */
    public void testAPartialBackendMapIsRejected() {
        String[] lines = ArrayUtils.append(wellFormed(), "dimension.format.backend.csv = LOCAL");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("parquet"));
    }

    /** An absence whose reason contradicts its own key would render a report that argues with itself. */
    public void testAnAbsenceWhoseReasonKindDisagreesWithItsKeyIsRejected() {
        String[] lines = ArrayUtils.append(wellFormed(), "dimension.error_mode.gap.skip_row = rule: not really a gap");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("gap:"));
    }

    /** Declaring the baseline missing cannot be true -- every vector carries the default. */
    public void testAnAbsenceOnTheDefaultValueIsRejected() {
        String[] lines = ArrayUtils.append(wellFormed(), "dimension.error_mode.gap.fail_fast = gap: nope");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureDimensions.parse(declaration(lines)));
        assertThat(e.getMessage(), containsString("effective default"));
    }

    /** A per-format absence wins over a bare one, so one value can be a rule here and a gap there. */
    public void testAPerFormatAbsenceWinsOverABareOne() {
        String[] lines = ArrayUtils.append(
            ArrayUtils.append(wellFormed(), "dimension.error_mode.gap.skip_row = gap: nothing writes a bad row yet"),
            "dimension.error_mode.rule.skip_row.parquet = rule: parquet rejects the file, not the row"
        );
        FixtureDimensions d = FixtureDimensions.parse(declaration(lines));
        assertThat(d.absenceReason("error_mode", "skip_row", "csv"), containsString("nothing writes"));
        assertThat(d.absenceReason("error_mode", "skip_row", "parquet"), containsString("rejects the file"));
        assertThat("a value with no absence is not licensed", d.absenceReason("error_mode", "fail_fast", "csv"), nullValue());
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
