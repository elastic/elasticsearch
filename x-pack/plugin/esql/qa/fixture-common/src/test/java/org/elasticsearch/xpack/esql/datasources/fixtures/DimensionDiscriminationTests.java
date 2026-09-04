/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Fails when pinning a dimension changes nothing observable.
 *
 * <p>The failure mode of a crossing is not a false red, it is a configuration that does not differ. A
 * vector can name a value, the audit can bless the cell, the suite can run it thousands of times, and
 * the bytes and settings can be identical to the default the whole way -- a pass that means nothing,
 * indistinguishable downstream from real coverage.
 *
 * <p>That is not hypothetical. Both fixture generators built TextRowRenderer with its 3-arg constructor,
 * so {@code quote=single} and {@code escape=tilde} vectors rendered with the DEFAULT characters while
 * announcing the pinned ones to the reader. And {@code schema_mode=declared_*} injected nothing on
 * sources-based layouts, so those cases ran the inferred baseline under a declared name. Both passed.
 * Neither was caught by a run; both were found by reading the code.
 *
 * <p>So every value a dimension declares must produce something observably different from its default.
 * What "observable" means depends on where the value lands, and each is checked below.
 */
public class DimensionDiscriminationTests extends ESTestCase {

    /**
     * A value routed into a setting must actually change that setting's map.
     *
     * <p>This is the cheap, general half: whatever the seam, if the dimension names a key then pinning an
     * off-default value has to alter what the reader, the directive, or the pragma is told. A dimension
     * that names a key and emits nothing is inert, and inert is what looks like coverage.
     */
    public void testEveryKeyedValueChangesTheSettingsItProduces() {
        FixtureDimensions dimensions = FixtureDimensions.get();
        List<String> inert = new ArrayList<>();
        int checked = 0;

        for (String dimension : dimensions.names()) {
            boolean keyed = dimensions.directiveKey(dimension) != null
                || dimensions.readKey(dimension) != null
                || dimensions.pragmaKey(dimension) != null;
            if (keyed == false) {
                continue;
            }
            // A DERIVED dimension's value is not a constant, so it cannot appear in a settings map: the
            // key names where the content lands and the content comes from the dataset. schema_mode rides
            // `mappings`, and directiveSettings deliberately emits nothing for it -- emitting the slot name
            // would put the literal "declared_open" where a schema belongs. Its discrimination is checked
            // by testDerivedValuesProduceDifferentContent instead, against the content it actually injects.
            if (dimensions.derivedFrom(dimension) != null) {
                continue;
            }
            for (String format : dimensions.appliesTo(dimension).isEmpty() ? FORMATS : dimensions.appliesTo(dimension)) {
                String baseline = dimensions.defaultValue(dimension, format);
                for (String value : dimensions.values(dimension)) {
                    if (value.equals(baseline)) {
                        continue;
                    }
                    checked++;
                    Map<String, String> at = settingsFor(dimensions, dimension, value, format);
                    Map<String, String> base = settingsFor(dimensions, dimension, baseline, format);
                    if (at.equals(base)) {
                        inert.add(
                            String.format(
                                Locale.ROOT,
                                "%s=%s@%s produces the same settings as its default [%s]: %s",
                                dimension,
                                value,
                                format,
                                baseline,
                                at
                            )
                        );
                    }
                }
            }
        }
        assertThat("no keyed cells were checked -- the gate would pass vacuously", checked, greaterThan(0));
        assertTrue("dimension values that change nothing:\n" + String.join("\n", inert), inert.isEmpty());
    }

    /**
     * A value that changes the BYTES must actually change them.
     *
     * <p>The settings check above cannot see this: {@code quote} announces itself through a read_key AND
     * decides what the writer emits, and it was announcing correctly while writing the default. Rendering
     * a probe row through each value and comparing output is what distinguishes the two.
     */
    public void testEveryGrammarValueChangesTheBytesItRenders() {
        FixtureDimensions dimensions = FixtureDimensions.get();
        List<String> inert = new ArrayList<>();
        int checked = 0;

        // A row that exercises all three grammar characters at once: it holds a delimiter candidate, a
        // quote candidate and text an escape would have to protect.
        Object[] probe = new Object[] { "a,b;c|d\te\"f'g", 1 };

        for (String dimension : List.of("delimiter", "quote", "escape")) {
            String baseline = dimensions.defaultValue(dimension, "csv");
            String base = renderProbe(dimensions, dimension, baseline, probe);
            for (String value : dimensions.values(dimension)) {
                if (value.equals(baseline)) {
                    continue;
                }
                checked++;
                if (renderProbe(dimensions, dimension, value, probe).equals(base)) {
                    inert.add(dimension + "=" + value + " renders byte-identically to its default [" + baseline + "]");
                }
            }
        }
        assertThat("no grammar cells were checked -- the gate would pass vacuously", checked, greaterThan(0));
        assertTrue("grammar values that change nothing:\n" + String.join("\n", inert), inert.isEmpty());
    }

    /**
     * A derived dimension must still discriminate -- in the content it injects, not in a settings key.
     *
     * <p>schema_mode is the only one today. declared_open and declared_closed differ by the `dynamic`
     * flag, and both must differ from inferred, which injects no mappings at all. This is the check that
     * would have caught declared_* no-opping: if the content were identical the vectors would be three
     * names for one configuration.
     */
    public void testDerivedValuesProduceDifferentContent() {
        FixtureDimensions dimensions = FixtureDimensions.get();
        // A synthetic schema rather than a fixture: the question is whether the two modes produce
        // different CONTENT, which does not need a real dataset -- and fixture-common's tests do not carry
        // the fixture data, so loading one would make the gate depend on a classpath it does not control.
        List<CsvFixtureParser.ColumnSpec> schema = List.of(
            new CsvFixtureParser.ColumnSpec("id", "integer"),
            new CsvFixtureParser.ColumnSpec("name", "keyword")
        );

        String open = DeclaredSchemas.mappingsJson(schema, true);
        String closed = DeclaredSchemas.mappingsJson(schema, false);
        assertNotNull("declared_open must produce a declaration", open);
        assertNotNull("declared_closed must produce a declaration", closed);
        assertNotEquals("declared_open and declared_closed must not be the same declaration", open, closed);
        assertTrue("both must name the dynamic flag they differ by", open.contains("dynamic"));

        // And inferred injects nothing, so it differs from both by construction. Pinned here so that a
        // future change making inferred inject something has to say so.
        assertEquals("inferred is the baseline", "inferred", dimensions.defaultValue("schema_mode", "csv"));
    }

    /** Read from the contract: the gate that guards copies caught this list being one of them. */
    /**
     * Every cell a seam can express must be carried by at least one vector.
     *
     * <p>Reachable and exercised are different claims, and the gap between them is silent: the audit
     * reports a cell covered because a seam CAN express it, while the crossing may never ask for it.
     * Eight cells sat in that gap -- quote and escape on tsv, partition_detection on tsv, ndjson and
     * parquet -- because a clique that does not contain `format` was pinned to ONE format, preferring
     * csv. Declared, capability rows present, seams serving them, and never run.
     */
    public void testEveryReachableCellIsCarriedByAVector() {
        FixtureDimensions dimensions = FixtureDimensions.get();
        Set<FixtureDimensions.Seam> all = EnumSet.allOf(FixtureDimensions.Seam.class);
        List<String> unexercised = new ArrayList<>();
        int reachable = 0;

        for (String format : FORMATS) {
            // Only formats a suite actually consumes. ORC's fixtures are generated in full and read by
            // nothing (dimension.format.rule.orc), so its cells are reachable through a seam and correctly
            // never run -- asking the crossing for them would be asking for vectors no suite can execute.
            if (FixtureCapabilities.formatIsConsumed(dimensions, format) == false) {
                continue;
            }
            Map<String, Set<String>> carried = new LinkedHashMap<>();
            for (Map<String, String> vector : dimensions.expressibleVectors(format, all)) {
                vector.forEach((slot, value) -> carried.computeIfAbsent(slot, k -> new LinkedHashSet<>()).add(value));
            }
            for (String dimension : dimensions.names()) {
                Set<String> scope = dimensions.appliesTo(dimension);
                if (scope.isEmpty() == false && scope.contains(format) == false) {
                    continue;
                }
                String baseline = dimensions.defaultValue(dimension, format);
                for (String value : dimensions.values(dimension)) {
                    if (value.equals(baseline) || dimensions.seamServes(dimension, value, format, all) == false) {
                        continue;
                    }
                    reachable++;
                    if (carried.getOrDefault(dimension, Set.of()).contains(value) == false) {
                        unexercised.add(dimension + "=" + value + "@" + format + " is reachable but no vector carries it");
                    }
                }
            }
        }
        assertThat("no reachable cells were found -- the gate would pass vacuously", reachable, greaterThan(0));
        assertTrue("reachable but never exercised:\n" + String.join("\n", unexercised), unexercised.isEmpty());
    }

    private static final List<String> FORMATS = FixtureDimensions.get().values("format");

    private static Map<String, String> settingsFor(FixtureDimensions dimensions, String dimension, String value, String format) {
        Map<String, String> vector = new LinkedHashMap<>();
        vector.put("format", format);
        vector.put(dimension, value);
        Map<String, String> all = new LinkedHashMap<>();
        all.putAll(dimensions.directiveSettings(vector));
        all.putAll(dimensions.readSettings(vector, format));
        all.putAll(dimensions.pragmaSettings(vector, format));
        return all;
    }

    /** Renders one row with a single grammar character re-pointed, everything else at its default. */
    private static String renderProbe(FixtureDimensions dimensions, String dimension, String value, Object[] row) {
        char delimiter = dimensions.charValue("delimiter", pick(dimensions, dimension, "delimiter", value, "csv"));
        char quote = dimensions.charValue("quote", pick(dimensions, dimension, "quote", value, "csv"));
        char escape = dimensions.charValue("escape", pick(dimensions, dimension, "escape", value, "csv"));
        TextRowRenderer renderer = new TextRowRenderer(delimiter, quote, escape, TextRowRenderer.Dialect.ESCAPED, false);
        return renderer.render(
            new CsvFixtureParser.CsvFixtureResult(
                List.of(new CsvFixtureParser.ColumnSpec("text", "keyword"), new CsvFixtureParser.ColumnSpec("n", "integer")),
                List.<Object[]>of(row)
            )
        );
    }

    private static String pick(FixtureDimensions dimensions, String pinned, String slot, String value, String format) {
        return pinned.equals(slot) ? value : dimensions.defaultValue(slot, format);
    }
}
