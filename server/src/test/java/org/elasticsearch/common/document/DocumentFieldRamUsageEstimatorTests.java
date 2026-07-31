/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.document;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class DocumentFieldRamUsageEstimatorTests extends ESTestCase {

    public void testEstimateIsNonZeroForEmptyField() {
        DocumentField field = new DocumentField("name", Collections.emptyList());
        assertThat(DocumentFieldRamUsageEstimator.estimate(field), greaterThan(0L));
    }

    public void testEstimateGrowsWithStringValueSize() {
        DocumentField small = new DocumentField("f", List.of("hi"));
        String largeValue = randomAlphaOfLength(4096);
        DocumentField large = new DocumentField("f", List.of(largeValue));

        long smallEstimate = DocumentFieldRamUsageEstimator.estimate(small);
        long largeEstimate = DocumentFieldRamUsageEstimator.estimate(large);
        long expectedCharPayload = (long) largeValue.length() * Character.BYTES;
        assertThat(largeEstimate - smallEstimate, greaterThanOrEqualTo(expectedCharPayload - 16L));
    }

    public void testEstimateGrowsWithCollectionSize() {
        List<Object> manyValues = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            manyValues.add("entry-" + i);
        }
        DocumentField large = new DocumentField("f", manyValues);
        DocumentField small = new DocumentField("f", List.of("one"));
        assertThat(DocumentFieldRamUsageEstimator.estimate(large), greaterThan(DocumentFieldRamUsageEstimator.estimate(small)));
    }

    public void testEstimateAccountsForNestedMapValues() {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("text", randomAlphaOfLength(64));
            entry.put("number", i);
            values.add(entry);
        }
        DocumentField nested = new DocumentField("f", values);
        DocumentField shallow = new DocumentField("f", List.of("just a small string"));
        assertThat(DocumentFieldRamUsageEstimator.estimate(nested), greaterThan(DocumentFieldRamUsageEstimator.estimate(shallow)));
    }

    public void testEstimateIncludesIgnoredValues() {
        DocumentField withoutIgnored = new DocumentField("f", List.of("v"), Collections.emptyList());
        DocumentField withIgnored = new DocumentField("f", List.of("v"), List.of(randomAlphaOfLength(2048)));
        assertThat(
            DocumentFieldRamUsageEstimator.estimate(withIgnored),
            greaterThan(DocumentFieldRamUsageEstimator.estimate(withoutIgnored))
        );
    }

    public void testEstimateNeverUnderCountsActualHeap() {
        List<Supplier<List<Object>>> shapes = List.of(
            () -> List.of("small"),
            () -> List.of(randomAlphaOfLength(1024)),
            () -> List.of(42L, 3.14d, true, 'x'),
            () -> List.of(new byte[10_000]),
            () -> List.of(new int[2_000]),
            () -> {
                ArrayList<Object> l = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    l.add("value-" + i);
                }
                return List.of(l);
            },
            () -> {
                LinkedList<Object> l = new LinkedList<>();
                for (int i = 0; i < 100; i++) {
                    l.add("v-" + i);
                }
                return List.of(l);
            },
            () -> {
                ArrayDeque<Object> d = new ArrayDeque<>();
                for (int i = 0; i < 100; i++) {
                    d.add("v-" + i);
                }
                return List.of(d);
            },
            () -> {
                HashMap<String, Object> m = new HashMap<>();
                for (int i = 0; i < 200; i++) {
                    m.put("k" + i, "v" + i);
                }
                return List.of(m);
            },
            () -> {
                HashMap<String, Object> m = new HashMap<>();
                for (int i = 0; i < 10_000; i++) {
                    m.put("k" + i, i);
                }
                return List.of(m);
            },
            () -> {
                LinkedHashMap<String, Object> m = new LinkedHashMap<>();
                for (int i = 0; i < 500; i++) {
                    m.put("k" + i, "v" + i);
                }
                return List.of(m);
            },
            () -> {
                TreeMap<String, Object> m = new TreeMap<>();
                for (int i = 0; i < 500; i++) {
                    m.put("k" + i, "v" + i);
                }
                return List.of(m);
            },
            () -> {
                HashSet<Object> s = new HashSet<>();
                for (int i = 0; i < 500; i++) {
                    s.add("v" + i);
                }
                return List.of(s);
            },
            () -> {
                LinkedHashSet<Object> s = new LinkedHashSet<>();
                for (int i = 0; i < 500; i++) {
                    s.add("v" + i);
                }
                return List.of(s);
            },
            () -> {
                TreeSet<Object> s = new TreeSet<>();
                for (int i = 0; i < 500; i++) {
                    s.add("v" + i);
                }
                return List.of(s);
            },
            () -> {
                ArrayList<Object> outer = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("id", i);
                    row.put("name", "row-" + i);
                    row.put("tags", List.of("a", "b", "c"));
                    outer.add(row);
                }
                return List.of(outer);
            },
            () -> List.of(new Object[] { "a", "b", 42, "c" })
        );

        for (Supplier<List<Object>> shape : shapes) {
            DocumentField field = new DocumentField("f", shape.get());
            long estimate = DocumentFieldRamUsageEstimator.estimate(field);
            long actual = RamUsageTester.ramUsed(field);
            assertThat(
                "estimate under-counts retained heap: estimate=" + estimate + " actual=" + actual + " for values=" + field.getValues(),
                estimate,
                greaterThanOrEqualTo(actual)
            );
        }
    }
}
