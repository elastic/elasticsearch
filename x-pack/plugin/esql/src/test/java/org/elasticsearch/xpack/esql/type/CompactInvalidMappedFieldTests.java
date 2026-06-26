/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.type.CompactInvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CompactInvalidMappedFieldTests extends ESTestCase {
    public void testParentWithCompactInvalidMappedFieldCanBeTransported() throws IOException {
        Map<DataType, Set<String>> input = new TreeMap<>(
            Map.of(DataType.KEYWORD, new LinkedHashSet<>(Set.of("idx_a")), DataType.TEXT, new LinkedHashSet<>(Set.of("idx_b")))
        );
        CompactInvalidMappedField child = CompactInvalidMappedField.mappedEverywhere("analyzed", input, new HashMap<>());

        EsField copiedChild = copyField(parentWithChild(child)).getProperties().get("analyzed");

        assertThat(copiedChild.getClass(), equalTo(EsField.class));
        assertThat(copiedChild.getName(), equalTo("analyzed"));
        assertThat(copiedChild.getDataType(), equalTo(DataType.UNSUPPORTED));
    }

    public void testParentWithInvalidMappedFieldCanBeTransported() throws IOException {
        InvalidMappedField child = new InvalidMappedField(
            "analyzed",
            new TreeMap<>(Map.of(DataType.KEYWORD.typeName(), Set.of("idx_a"), DataType.TEXT.typeName(), Set.of("idx_b")))
        );

        EsField copiedChild = copyField(parentWithChild(child)).getProperties().get("analyzed");

        assertThat(copiedChild.getClass(), equalTo(EsField.class));
        assertThat(copiedChild.getName(), equalTo("analyzed"));
        assertThat(copiedChild.getDataType(), equalTo(DataType.UNSUPPORTED));
    }

    public void testKeepsAllIndicesWhenAtOrBelowLimit() {
        Map<DataType, Set<String>> input = Map.of(
            DataType.KEYWORD,
            new LinkedHashSet<>(Set.of("idx_a", "idx_b")),
            DataType.LONG,
            new LinkedHashSet<>(Set.of("idx_c", "idx_d", "idx_e"))
        );

        CompactInvalidMappedField field = CompactInvalidMappedField.mappedEverywhere("f", input, new HashMap<>());

        assertMap(
            field.getTypesToIndices(),
            matchesMap().entry(DataType.KEYWORD.typeName(), Set.of("idx_a", "idx_b"))
                .entry(DataType.LONG.typeName(), Set.of("idx_c", "idx_d", "idx_e"))
        );
    }

    public void testTruncatesAboveLimitAndAddsEllipsisSentinel() {
        Set<String> manyIndices = IntStream.range(0, 5_000)
            .mapToObj(i -> Strings.format("idx_%05d", i))
            .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<DataType, Set<String>> input = Map.of(DataType.KEYWORD, manyIndices);

        CompactInvalidMappedField field = CompactInvalidMappedField.mappedEverywhere("f", input, new HashMap<>());

        assertMap(
            field.getTypesToIndices(),
            matchesMap().entry(DataType.KEYWORD.typeName(), Set.of("idx_00000", "idx_00001", "idx_00002", "..."))
        );
    }

    public void testErrorMessageReflectsFullInputCountEvenAfterTruncation() {
        Set<String> manyIndices = IntStream.range(0, 5_000)
            .mapToObj(i -> Strings.format("idx_%05d", i))
            .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<DataType, Set<String>> input = new TreeMap<>(Map.of(DataType.KEYWORD, manyIndices));

        String message = CompactInvalidMappedField.mappedEverywhere("f", input, new HashMap<>()).errorMessage();

        assertThat(message, containsString("[1] incompatible types"));
        assertThat(message, containsString("[idx_00000, idx_00001, idx_00002]"));
        assertThat(message, containsString("[" + (5_000 - 3) + "] other indices"));
    }

    public void testErrorMessageMatchesInvalidMappedFieldForSmallInputs() {
        Set<String> kwIndices = new LinkedHashSet<>(Set.of("idx_a", "idx_b"));
        Set<String> longIndices = new LinkedHashSet<>(Set.of("idx_c"));
        Map<DataType, Set<String>> compactInput = new TreeMap<>(Map.of(DataType.KEYWORD, kwIndices, DataType.LONG, longIndices));
        Map<String, Set<String>> legacyInput = new TreeMap<>(
            Map.of(DataType.KEYWORD.typeName(), kwIndices, DataType.LONG.typeName(), longIndices)
        );

        assertThat(
            CompactInvalidMappedField.mappedEverywhere("f", compactInput, new HashMap<>()).errorMessage(),
            equalTo(new InvalidMappedField("f", legacyInput).errorMessage())
        );
    }

    public void testPotentiallyUnmappedFlagAndMessageInsistOnKeyword() {
        Map<DataType, Set<String>> input = new TreeMap<>(Map.of(DataType.LONG, new LinkedHashSet<>(Set.of("idx_a"))));

        CompactInvalidMappedField field = CompactInvalidMappedField.potentiallyUnmapped("f", input, new HashMap<>());

        assertThat(field.isPotentiallyUnmapped(), equalTo(true));
        assertThat(field.errorMessage(), containsString("[keyword] due to loading from _source"));
        assertMap(field.getTypesToIndices(), matchesMap().entry(DataType.LONG.typeName(), Set.of("idx_a")));
    }

    public void testTypesReflectsKeysOfTruncatedMap() {
        Map<DataType, Set<String>> input = new TreeMap<>(
            Map.of(
                DataType.KEYWORD,
                IntStream.range(0, 100).mapToObj(i -> "k" + i).collect(Collectors.toSet()),
                DataType.LONG,
                Set.of("only")
            )
        );

        assertThat(
            CompactInvalidMappedField.mappedEverywhere("f", input, new HashMap<>()).types(),
            containsInAnyOrder(DataType.KEYWORD, DataType.LONG)
        );
    }

    private static KeywordEsField parentWithChild(EsField child) {
        return new KeywordEsField("my_field", Map.of("analyzed", child), true, 256, false, false, EsField.TimeSeriesFieldType.NONE);
    }

    private static EsField copyField(EsField field) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput(); var pso = new PlanStreamOutput(output, EsqlTestUtils.TEST_CFG)) {
            field.writeTo(pso);
            try (
                var input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(List.of()));
                var psi = new PlanStreamInput(
                    input,
                    input.namedWriteableRegistry(),
                    EsqlTestUtils.TEST_CFG,
                    new SerializationTestUtils.TestNameIdMapper()
                )
            ) {
                return EsField.readFrom(psi);
            }
        }
    }
}
