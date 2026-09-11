/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureDimensions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.empty;

/**
 * The fixture crossing carries a MIRROR of this plugin's format config keys, and asserting they agree
 * has to happen here because only this module can see both.
 *
 * <p>The crossing needs the set to decide whether a dataset may be registered under a {@code ?} glob:
 * elastic/esql-planning#1841 truncates the object key at the {@code ?}, so a dataset carrying a
 * format-specific key fails registration. {@code fixture-common} cannot import the plugin -- it is
 * dependency-free by design so it stays off the ORC and Parquet generator classpaths, which isolate
 * their Hadoop jars -- so the list is declared as {@code format_specific_keys} and copied by hand.
 *
 * <p>A hand-copied list drifts, and it already did: {@code column_prefix} was missing while two routed
 * specs declared it, and they escaped a 400 only because those directives happen to also declare
 * {@code header_row}. That was found by reading, which is not a gate. Adding a key here without adding
 * it there now fails on precommit rather than as a 400 in whichever suite reaches the pair first.
 */
public class CsvFormatConfigKeysMirrorTests extends ESTestCase {

    public void testEveryFormatConfigKeyIsInTheCrossingsMirror() {
        Set<String> mirror = FixtureDimensions.get().formatSpecificKeys();
        List<String> missing = new ArrayList<>();
        for (String key : CsvDataSourcePlugin.FORMAT_CONFIG_KEYS) {
            if (mirror.contains(key) == false) {
                missing.add(key);
            }
        }
        assertThat(
            "format_specific_keys in fixture-dimensions.properties must list every key this plugin accepts, "
                + "or the crossing registers a glob pair the validator then rejects",
            missing,
            empty()
        );
    }
}
