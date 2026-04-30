/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.type.CompactInvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
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
import static org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField.ESQL_MULTI_TYPE_ES_FIELD_2;
import static org.hamcrest.Matchers.lessThan;

// FIXME(gal, NOCOMMIT) Go over these docs
/**
 * End-to-end check that an analyzed plan over many union-typed fields, each conflicting across thousands of indices, retains substantially
 * less memory under {@link CompactMultiTypeEsField} (paired with {@link CompactInvalidMappedField}'s truncated index lists) than under
 * the legacy {@link InvalidMappedField} (keyed per-index).
 */
public class MultiTypeEsFieldMemoryTests extends ESTestCase {
    private static final int NUM_INDICES = 5_000;
    private static final int NUM_CONFLICTING_FIELDS = 50;

    /**
     * {@link RamUsageTester} walks reflectively, which fails on JDK-internal classes (e.g. {@code sun.util.locale.BaseLocale}) that
     * aren't opened to unnamed modules. The plan transitively references a {@link Locale} and a {@link ZoneId} via the analyzer's
     * {@code Configuration}, so we treat those as opaque as they're irrelevant to the union-type memory we care about here.
     */
    private static final RamUsageTester.Accumulator ACCUMULATOR = new RamUsageTester.Accumulator() {
        @Override
        public long accumulateObject(Object o, long shallowSize, Map<Field, Object> fieldValues, Collection<Object> queue) {
            return o instanceof Locale || o instanceof ZoneId ? shallowSize : super.accumulateObject(o, shallowSize, fieldValues, queue);
        }
    };

    public void testV2AnalyzedPlanIsAtLeastTenTimesSmallerThanLegacy() {
        String evalAssignments = IntStream.range(0, MultiTypeEsFieldMemoryTests.NUM_CONFLICTING_FIELDS)
            .mapToObj(i -> "id_" + i + "_kw = id_" + i + "::keyword")
            .collect(Collectors.joining(", "));
        String keepFields = IntStream.range(0, MultiTypeEsFieldMemoryTests.NUM_CONFLICTING_FIELDS)
            .mapToObj(i -> "id_" + i + "_kw")
            .collect(Collectors.joining(", "));
        String query = "FROM idx* | EVAL " + evalAssignments + " | KEEP " + keepFields + " | LIMIT 1";

        assertThat(getBytesUsed(true, query) * 10L, lessThan(getBytesUsed(false, query)));
    }

    private static long getBytesUsed(boolean compact, String query) {
        TransportVersion transportVersion = compact
            ? TransportVersionUtils.randomVersionSupporting(ESQL_MULTI_TYPE_ES_FIELD_2)
            : TransportVersionUtils.randomVersionNotSupporting(ESQL_MULTI_TYPE_ES_FIELD_2);
        LogicalPlan plan = analyzer().addIndex(unionTypedIndex(compact)).minimumTransportVersion(transportVersion).query(query);
        return RamUsageTester.ramUsed(plan, ACCUMULATOR);
    }

    private static IndexResolution unionTypedIndex(boolean compact) {
        Map<String, IndexMode> indexNamesWithModes = new HashMap<>();
        for (int i = 0; i < MultiTypeEsFieldMemoryTests.NUM_INDICES; i++) {
            indexNamesWithModes.put("idx_" + i, IndexMode.STANDARD);
        }
        Map<String, EsField> mapping = new HashMap<>();
        for (int i = 0; i < MultiTypeEsFieldMemoryTests.NUM_CONFLICTING_FIELDS; i++) {
            String fieldName = "id_" + i;
            Set<String> kwIndices = new HashSet<>();
            Set<String> intIndices = new HashSet<>();
            for (int j = 0; j < MultiTypeEsFieldMemoryTests.NUM_INDICES; j++) {
                (j % 2 == 0 ? kwIndices : intIndices).add("idx_" + j);
            }
            mapping.put(
                fieldName,
                compact
                    ? new CompactInvalidMappedField(fieldName, Map.of(DataType.KEYWORD, kwIndices, DataType.INTEGER, intIndices))
                    : new InvalidMappedField(
                        fieldName,
                        Map.of(DataType.KEYWORD.typeName(), kwIndices, DataType.INTEGER.typeName(), intIndices)
                    )
            );
        }
        return IndexResolution.valid(new EsIndex("idx*", mapping, indexNamesWithModes, Map.of(), Map.of()));
    }
}
