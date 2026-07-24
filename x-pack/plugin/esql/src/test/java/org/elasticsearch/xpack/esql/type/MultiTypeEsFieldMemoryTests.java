/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.CompactInvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField.CompactMultiTypeEsField;
import static org.hamcrest.Matchers.lessThan;

/**
 * Memory comparisons between the compact and legacy variants of the two field types that union-typed columns flow through:
 * <ul>
 *   <li>{@link CompactInvalidMappedField} vs {@link InvalidMappedField} as the source-type field produced by {@link IndexResolver}.</li>
 *   <li>{@link CompactMultiTypeEsField} vs {@link MultiTypeEsField} as the resolved {@link UnionTypeEsField} retained by the analyzed
 *       {@link LogicalPlan}.</li>
 * </ul>
 */
public class MultiTypeEsFieldMemoryTests extends ESTestCase {
    private static final int NUM_INDICES = 1000;
    private static final int NUM_CONFLICTING_FIELDS = 500;

    /**
     * Approximate retained bytes per {@link Map}/{@link Collection} entry. Covers a {@code HashMap.Node} or {@code TreeMap.Entry}
     * (~48–56 B aligned) plus its share of the backing slot array (~load-factor-weighted reference).
     */
    private static final long APPROX_BYTES_PER_ENTRY = 48L;

    /**
     * <ol>
     *   <li>{@link RamUsageTester#handleOther} short-circuits every {@link Map} and {@link Iterable} in the {@code java.*} module to
     * {@link RamUsageTester.Accumulator#accumulateArray}, billing only {@code shallow + one array header} and walking a flattened
     * key/value list. The underlying {@code HashMap.Node}/{@code TreeMap.Entry} storage—which is most of the production memory
     * cost of {@link MultiTypeEsField}'s per-index conversion map and {@link InvalidMappedField}'s per-index sets—is never charged.</li>
     *   <li>{@link RamUsageTester} walks reflectively, which fails on JDK-internal classes (e.g. {@code sun.util.locale.BaseLocale})
     *       that aren't opened to unnamed modules. The analyzer's {@code Configuration} transitively references a {@link Locale} and
     *       a {@link ZoneId}, so we treat those as opaque.</li>
     *   <li>{@link Reference} values are filtered out at queue time rather than on pop: {@code RamUsageTester#handleOther} builds a
     *       class cache before consulting the accumulator.</li>
     * </ol>
     */
    private static final RamUsageTester.Accumulator ACCUMULATOR = new RamUsageTester.Accumulator() {
        @Override
        public long accumulateObject(Object o, long shallowSize, Map<Field, Object> fieldValues, Collection<Object> queue) {
            if (o instanceof Locale || o instanceof ZoneId) {
                return shallowSize;
            }
            for (Object value : fieldValues.values()) {
                if (value instanceof Reference<?> == false) {
                    queue.add(value);
                }
            }
            return shallowSize;
        }

        @Override
        public long accumulateArray(Object obj, long shallowSize, List<Object> values, Collection<Object> queue) {
            queue.addAll(values);
            if (obj.getClass().isArray()) {
                return shallowSize;
            }
            return switch (obj) {
                case Map<?, ?> map -> shallowSize + map.size() * APPROX_BYTES_PER_ENTRY;
                case Collection<?> collection -> shallowSize + collection.size() * APPROX_BYTES_PER_ENTRY;
                default -> shallowSize;
            };
        }
    };

    public void testCompactInvalidMappedFieldIsAtLeastTenTimesSmallerThanLegacy() {
        Map<String, EsField> compactFields = resolvedSourceFields(versionSupportingCompact());
        Map<String, EsField> legacyFields = resolvedSourceFields(versionNotSupportingCompact());

        long compactBytes = RamUsageTester.ramUsed(compactFields, ACCUMULATOR);
        long legacyBytes = RamUsageTester.ramUsed(legacyFields, ACCUMULATOR);

        assertThat(compactBytes * 10L, lessThan(legacyBytes));
        assertThat(compactBytes, lessThan(400_000L));
    }

    public void testCompactMultiTypeEsFieldIsAtLeastTenTimesSmallerThanLegacy() {
        List<UnionTypeEsField> compactFields = analyzedUnionTypeFields(versionSupportingCompact());
        List<UnionTypeEsField> legacyFields = analyzedUnionTypeFields(versionNotSupportingCompact());

        long compactBytes = RamUsageTester.ramUsed(compactFields, ACCUMULATOR);
        long legacyBytes = RamUsageTester.ramUsed(legacyFields, ACCUMULATOR);

        assertThat(compactBytes * 10L, lessThan(legacyBytes));
        assertThat(compactBytes, lessThan(600_000L));
    }

    private static TransportVersion versionSupportingCompact() {
        return TransportVersionUtils.randomVersionSupporting(CompactMultiTypeEsField);
    }

    private static TransportVersion versionNotSupportingCompact() {
        return TransportVersionUtils.randomVersionNotSupporting(CompactMultiTypeEsField);
    }

    private static Map<String, EsField> resolvedSourceFields(TransportVersion transportVersion) {
        return unionTypedIndex(transportVersion).get().mapping();
    }

    private static List<UnionTypeEsField> analyzedUnionTypeFields(TransportVersion transportVersion) {
        LogicalPlan plan = analyzer().addIndex(unionTypedIndex(transportVersion))
            .minimumTransportVersion(transportVersion)
            .query(unionTypeQuery());
        Set<UnionTypeEsField> uniqueByIdentity = Collections.newSetFromMap(new IdentityHashMap<>());
        plan.forEachExpressionDown(FieldAttribute.class, fa -> {
            if (fa.field() instanceof UnionTypeEsField utef) {
                uniqueByIdentity.add(utef);
            }
        });
        return new ArrayList<>(uniqueByIdentity);
    }

    private static String unionTypeQuery() {
        String evalAssignments = IntStream.range(0, NUM_CONFLICTING_FIELDS)
            .mapToObj(i -> "id_" + i + "_kw = id_" + i + "::keyword")
            .collect(Collectors.joining(", "));
        String keepFields = IntStream.range(0, NUM_CONFLICTING_FIELDS).mapToObj(i -> "id_" + i + "_kw").collect(Collectors.joining(", "));
        return "FROM idx* | EVAL " + evalAssignments + " | KEEP " + keepFields + " | LIMIT 1";
    }

    private static IndexResolution unionTypedIndex(TransportVersion transportVersion) {
        List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>(NUM_INDICES);
        for (int i = 0; i < NUM_INDICES; i++) {
            String fieldType = i % 2 == 0 ? "keyword" : "integer";
            Map<String, IndexFieldCapabilities> fields = IntStream.range(0, NUM_CONFLICTING_FIELDS)
                .boxed()
                .collect(Collectors.toMap(j -> "id_" + j, j -> new IndexFieldCapabilitiesBuilder("id_" + j, fieldType).build()));
            // Unique per-index hash so the resolver doesn't dedup the responses; otherwise it would only see one entry per source type
            // and lose track of which indices contributed which type.
            String mappingHash = "hash_" + i;
            indexResponses.add(new FieldCapabilitiesIndexResponse("idx_" + i, mappingHash, fields, false, IndexMode.STANDARD));
        }
        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(indexResponses, List.of());
        IndexResolver.FieldsInfo info = new IndexResolver.FieldsInfo(caps, transportVersion, false, false, false, false, true);
        return IndexResolver.mergedMappings("idx*", false, info, false, IndexResolver.DO_NOT_GROUP);
    }
}
