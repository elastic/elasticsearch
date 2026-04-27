/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField2;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;

public class InvalidMappedField2Tests extends ESTestCase {

    public void testKeepsAllIndicesWhenAtOrBelowLimit() {
        Map<String, Set<String>> input = Map.of(
            DataType.KEYWORD.typeName(),
            new LinkedHashSet<>(Set.of("idx_a", "idx_b")),
            DataType.LONG.typeName(),
            new LinkedHashSet<>(Set.of("idx_c", "idx_d", "idx_e"))
        );

        InvalidMappedField2 field = new InvalidMappedField2("f", input);

        Map<String, Set<String>> stored = field.getTypesToIndices();
        assertThat(stored.keySet(), containsInAnyOrder(DataType.KEYWORD.typeName(), DataType.LONG.typeName()));
        assertThat(stored.get(DataType.KEYWORD.typeName()), containsInAnyOrder("idx_a", "idx_b"));
        assertThat(stored.get(DataType.LONG.typeName()), containsInAnyOrder("idx_c", "idx_d", "idx_e"));
        for (Set<String> indices : stored.values()) {
            assertThat(indices, not(containsInAnyOrder(InvalidMappedField2.ELLIPSIS)));
        }
    }

    public void testTruncatesAboveLimitAndAddsEllipsisSentinel() {
        Set<String> manyIndices = IntStream.range(0, 5_000)
            .mapToObj(i -> String.format("idx_%05d", i))
            .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, Set<String>> input = Map.of(DataType.KEYWORD.typeName(), manyIndices);

        InvalidMappedField2 field = new InvalidMappedField2("f", input);

        Set<String> stored = field.getTypesToIndices().get(DataType.KEYWORD.typeName());
        assertThat(stored, equalTo(Set.of("idx_00000", "idx_00001", "idx_00002", InvalidMappedField2.ELLIPSIS)));
    }

    public void testErrorMessageReflectsFullInputCountEvenAfterTruncation() {
        Set<String> manyIndices = IntStream.range(0, 5_000)
            .mapToObj(i -> String.format("idx_%05d", i))
            .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, Set<String>> input = new TreeMap<>(Map.of(DataType.KEYWORD.typeName(), manyIndices));

        String message = new InvalidMappedField2("f", input).errorMessage();

        assertThat(message, containsString("[1] incompatible types"));
        assertThat(message, containsString("[idx_00000, idx_00001, idx_00002]"));
        assertThat(message, containsString("[" + (5_000 - 3) + "] other indices"));
    }

    public void testErrorMessageMatchesInvalidMappedFieldForSmallInputs() {
        Map<String, Set<String>> input = new TreeMap<>(
            Map.of(
                DataType.KEYWORD.typeName(),
                new LinkedHashSet<>(Set.of("idx_a", "idx_b")),
                DataType.LONG.typeName(),
                new LinkedHashSet<>(Set.of("idx_c"))
            )
        );

        assertThat(new InvalidMappedField2("f", input).errorMessage(), equalTo(new InvalidMappedField("f", input).errorMessage()));
    }

    public void testPotentiallyUnmappedFlagAndMessageInsistOnKeyword() {
        Map<String, Set<String>> input = new TreeMap<>(Map.of(DataType.LONG.typeName(), new LinkedHashSet<>(Set.of("idx_a"))));

        InvalidMappedField2 field = InvalidMappedField2.potentiallyUnmapped("f", input);

        assertThat(field.isPotentiallyUnmapped(), equalTo(true));
        assertThat(field.errorMessage(), containsString("[keyword] due to loading from _source"));
        assertThat(field.getTypesToIndices(), hasEntry(DataType.LONG.typeName(), Set.of("idx_a")));
    }

    public void testTypesReflectsKeysOfTruncatedMap() {
        Map<String, Set<String>> input = new TreeMap<>(
            Map.of(
                DataType.KEYWORD.typeName(),
                IntStream.range(0, 100).mapToObj(i -> "k" + i).collect(Collectors.toSet()),
                DataType.LONG.typeName(),
                Set.of("only")
            )
        );

        InvalidMappedField2 field = new InvalidMappedField2("f", input);

        assertThat(field.types(), containsInAnyOrder(DataType.KEYWORD, DataType.LONG));
    }

}
