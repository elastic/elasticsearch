/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureDimensions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.empty;

/**
 * The NDJSON half of the same mirror the CSV plugin gates -- see
 * {@code CsvFormatConfigKeysMirrorTests} for why the copy exists and why the assertion lives in the
 * plugin rather than in the crossing.
 *
 * <p>Separate because the declared list is the UNION of every format's keys and no single module sees
 * all of them. {@code segment_size} is NDJSON's alone, so a gate that only asked CSV would let it be
 * dropped from the mirror without a word.
 */
public class NdJsonFormatConfigKeysMirrorTests extends ESTestCase {

    public void testEveryFormatConfigKeyIsInTheCrossingsMirror() {
        Set<String> mirror = FixtureDimensions.get().formatSpecificKeys();
        List<String> missing = new ArrayList<>();
        for (String key : NdJsonDataSourcePlugin.FORMAT_CONFIG_KEYS) {
            if (mirror.contains(key) == false) {
                missing.add(key);
            }
        }
        assertThat("format_specific_keys must list every key this plugin accepts", missing, empty());
    }
}
