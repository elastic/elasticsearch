/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.lessThan;

/**
 * End-to-end check that an analyzed plan over many union-typed fields, each conflicting across thousands of indices, retains substantially
 * less memory under {@link CompactMultiTypeEsField} (paired with {@link InvalidMappedField#compact}'s truncated index lists) than under
 * the legacy {@link org.elasticsearch.xpack.esql.core.type.MultiTypeEsField} (keyed per-index) paired with a full {@link InvalidMappedField}.
 *
 * <p>The cost we're targeting is {@code O(num_fields * num_indices)}: each conflicting field expands into its own per-index conversion
 * map under legacy, and per-type under v2. With many fields the constant overhead from {@link EsIndex#indexNameWithModes} becomes a
 * small fixed tax, and the savings show up cleanly at plan-total scope.
 *
 * <p>One subtlety in the fixture: each conflicting field gets its <em>own</em> {@code typesToIndices} map. Sharing one across all fields
 * (even though it's logically the same content) makes {@link RamUsageTester}'s identity-based dedup collapse all 50 analyzer-derived
 * conversion structures down to roughly one field's worth, which masks the savings. Production index resolutions don't share these maps
 * across fields - they're built per-field from {@code FieldCapabilitiesResponse} - so the per-field copy here is the realistic case.
 */
public class MultiTypeEsFieldMemoryTests extends ESTestCase {
    private static final int NUM_INDICES = 5_000;
    private static final int NUM_CONFLICTING_FIELDS = 50;

    /**
     * {@link RamUsageTester} walks reflectively, which fails on JDK-internal classes (e.g. {@code sun.util.locale.BaseLocale}) that
     * aren't opened to unnamed modules. The plan transitively references a {@link Locale} and a {@link ZoneId} via the analyzer's
     * {@code Configuration}, so we treat those as opaque - they're irrelevant to the union-type memory we care about here.
     */
    private static final RamUsageTester.Accumulator ACCUMULATOR = new RamUsageTester.Accumulator() {
        @Override
        public long accumulateObject(Object o, long shallowSize, Map<Field, Object> fieldValues, Collection<Object> queue) {
            return o instanceof Locale || o instanceof ZoneId ? shallowSize : super.accumulateObject(o, shallowSize, fieldValues, queue);
        }
    };

    public void testV2AnalyzedPlanIsAtLeastTenTimesSmallerThanLegacy() {
        String query = buildExplicitConversionQuery(NUM_CONFLICTING_FIELDS);

        LogicalPlan legacyPlan = analyzer().addIndex(unionTypedIndex(false))
            .minimumTransportVersion(TransportVersionUtils.randomVersionNotSupporting(CompactMultiTypeEsField.ESQL_MULTI_TYPE_ES_FIELD_2))
            .query(query);
        LogicalPlan v2Plan = analyzer().addIndex(unionTypedIndex(true))
            .minimumTransportVersion(CompactMultiTypeEsField.ESQL_MULTI_TYPE_ES_FIELD_2)
            .query(query);

        long legacyBytes = RamUsageTester.ramUsed(legacyPlan, ACCUMULATOR);
        long v2Bytes = RamUsageTester.ramUsed(v2Plan, ACCUMULATOR);
        assertThat(v2Bytes * 10L, lessThan(legacyBytes));
    }

    /**
     * Build a query that forces the analyzer to materialize a {@code MultiTypeEsField}/{@link CompactMultiTypeEsField} for every
     * {@code id_<i>} field by explicitly casting each to keyword.
     */
    private static String buildExplicitConversionQuery(int numFields) {
        String evalAssignments = IntStream.range(0, numFields)
            .mapToObj(i -> "id_" + i + "_kw = id_" + i + "::keyword")
            .collect(Collectors.joining(", "));
        String keepFields = IntStream.range(0, numFields).mapToObj(i -> "id_" + i + "_kw").collect(Collectors.joining(", "));
        return "FROM idx* | EVAL " + evalAssignments + " | KEEP " + keepFields + " | LIMIT 1";
    }

    /**
     * Build a fake "idx*" pattern with {@code numIndices} concrete indices and {@code numConflictingFields} fields {@code id_0..id_<n>},
     * each with type {@code keyword} in half of the indices and {@code integer} in the other half. When {@code compact} is true the
     * conflicting fields are built from {@link InvalidMappedField#compact} (truncated index lists), matching what a v2-capable coordinator
     * produces; otherwise the full {@link InvalidMappedField} is used.
     */
    private static IndexResolution unionTypedIndex(boolean compact) {
        Map<String, IndexMode> indexNamesWithModes = new HashMap<>();
        for (int i = 0; i < MultiTypeEsFieldMemoryTests.NUM_INDICES; i++) {
            indexNamesWithModes.put("idx_" + i, IndexMode.STANDARD);
        }
        Map<String, EsField> mapping = new HashMap<>();
        for (int i = 0; i < MultiTypeEsFieldMemoryTests.NUM_CONFLICTING_FIELDS; i++) {
            String fieldName = "id_" + i;
            Map<String, Set<String>> perFieldTypesToIndices = new HashMap<>();
            perFieldTypesToIndices.put("keyword", new HashSet<>());
            perFieldTypesToIndices.put("integer", new HashSet<>());
            for (int j = 0; j < MultiTypeEsFieldMemoryTests.NUM_INDICES; j++) {
                perFieldTypesToIndices.get(j % 2 == 0 ? "keyword" : "integer").add("idx_" + j);
            }
            mapping.put(
                fieldName,
                compact
                    ? InvalidMappedField.compact(fieldName, perFieldTypesToIndices)
                    : new InvalidMappedField(fieldName, perFieldTypesToIndices)
            );
        }
        return IndexResolution.valid(new EsIndex("idx*", mapping, indexNamesWithModes, Map.of(), Map.of()));
    }
}
