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
 * <p>That is not hypothetical. {@code schema_mode=declared_*} injected nothing on sources-based layouts,
 * so those cases ran the inferred baseline under a declared name, and a renderer built with the wrong
 * constructor wrote the DEFAULT grammar characters while announcing the pinned ones to the reader. Both
 * passed. Neither was caught by a run; both were found by reading the code.
 *
 * <p>The byte-level half of this check lives with the fixture generators and is not in this module: what
 * is checked here is everything that lands in a SETTING, and that every reachable cell is carried.
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
            // nothing (dimension.data.format.rule.orc), so its cells are reachable through a seam and correctly
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

    private static final List<String> FORMATS = FixtureDimensions.get().values("data.format");

    private static Map<String, String> settingsFor(FixtureDimensions dimensions, String dimension, String value, String format) {
        Map<String, String> vector = new LinkedHashMap<>();
        vector.put("data.format", format);
        vector.put(dimension, value);
        Map<String, String> all = new LinkedHashMap<>();
        all.putAll(dimensions.directiveSettings(vector));
        all.putAll(dimensions.readSettings(vector, format));
        all.putAll(dimensions.pragmaSettings(vector, format));
        return all;
    }
}
