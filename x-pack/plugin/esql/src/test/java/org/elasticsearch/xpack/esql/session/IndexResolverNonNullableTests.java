/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests which fields {@link IndexResolver#mergedMappings} reports in {@link EsField#isStaticallyNonNullable()}: a field qualifies only
 * if every index that the query reads has it, and each of them guarantees a value for every document.
 */
public class IndexResolverNonNullableTests extends ESTestCase {

    public void testFlaggedInEveryIndex() {
        EsIndex index = resolve(false, index("a", field("x", "long", true)), index("b", field("x", "long", true)));
        assertThat(nonNullableFields(index), equalTo(Set.of("x")));
    }

    public void testOneIndexWithoutTheGuaranteeDisqualifiesTheField() {
        EsIndex index = resolve(false, index("a", field("x", "long", true)), index("b", field("x", "long", false)));
        assertThat(nonNullableFields(index), empty());
    }

    public void testNoGuaranteeByDefault() {
        EsIndex index = resolve(false, index("a", field("x", "long", false)));
        assertThat(nonNullableFields(index), empty());
    }

    /** A field that an index does not have is null for its documents, unlike for the single-value guarantee. */
    public void testFieldMissingFromAnIndexIsNotNonNullable() {
        EsIndex index = resolve(false, index("a", field("x", "long", true)), index("b", field("y", "long", true)));
        assertThat(nonNullableFields(index), empty());
    }

    /** The field caps of indices with an equal mapping hash are listed once, so the field is not missing from the second index. */
    public void testIndicesWithTheSameMappingHashCountOnce() {
        EsIndex index = resolve(
            false,
            indexWithHash("a", "same", field("x", "long", true)),
            indexWithHash("b", "same", field("x", "long", true))
        );
        assertThat(nonNullableFields(index), equalTo(Set.of("x")));
    }

    public void testSameHashAndAnIndexWithAnotherMappingWithoutTheField() {
        EsIndex index = resolve(
            false,
            indexWithHash("a", "same", field("x", "long", true)),
            indexWithHash("b", "same", field("x", "long", true)),
            indexWithHash("c", "other", field("y", "long", true))
        );
        assertThat(nonNullableFields(index), empty());
    }

    /** A lookup index is resolved without reporting the flag: a left join nulls the fields of the rows that do not match. */
    public void testNothingIsReportedWhenNotAskedFor() {
        EsIndex index = resolve(false, false, index("a", field("x", "long", true)));
        assertThat(nonNullableFields(index), empty());
    }

    public void testDateAndBooleanQualify() {
        EsIndex index = resolve(false, index("a", field("d", "date", true), field("b", "boolean", true), field("n", "date_nanos", true)));
        assertThat(nonNullableFields(index), equalTo(Set.of("d", "b", "n")));
    }

    public void testSmallNumbersQualifyAndAreWidened() {
        EsIndex index = resolve(false, index("a", field("s", "short", true), field("by", "byte", true)));
        assertThat(nonNullableFields(index), equalTo(Set.of("s", "by")));
    }

    public void testDottedFieldUsesFullName() {
        EsIndex index = resolve(false, index("a", field("outer.inner", "long", true)));
        assertThat(nonNullableFields(index), equalTo(Set.of("outer.inner")));
    }

    public void testTypeConflictIsNotNonNullable() {
        EsIndex index = resolve(false, index("a", field("x", "long", true)), index("b", field("x", "keyword", true)));
        assertThat(nonNullableFields(index), empty());
    }

    /** With unmapped field tracking, a field that one index does not map is read from {@code _source} there, so it can be null. */
    public void testPartiallyUnmappedFieldIsNotNonNullable() {
        EsIndex index = resolve(true, index("a", field("x", "long", true)), index("b", field("y", "long", true)));
        assertThat(nonNullableFields(index), empty());
    }

    /** The full names of the fields that carry the guarantee. */
    private static Set<String> nonNullableFields(EsIndex index) {
        Set<String> names = new HashSet<>();
        collect("", index.mapping(), names);
        return names;
    }

    private static void collect(String prefix, Map<String, EsField> fields, Set<String> names) {
        fields.forEach((name, field) -> {
            String fullName = prefix + name;
            if (field.isStaticallyNonNullable()) {
                names.add(fullName);
            }
            collect(fullName + ".", field.getProperties(), names);
        });
    }

    private static IndexFieldCapabilities field(String name, String type, boolean nonNullable) {
        return new IndexFieldCapabilitiesBuilder(name, type).isStaticallyNonNullable(nonNullable).build();
    }

    private static FieldCapabilitiesIndexResponse index(String name, IndexFieldCapabilities... fields) {
        return indexWithHash(name, name, fields);
    }

    private static FieldCapabilitiesIndexResponse indexWithHash(String name, String mappingHash, IndexFieldCapabilities... fields) {
        Map<String, IndexFieldCapabilities> byName = new HashMap<>();
        Arrays.stream(fields).forEach(f -> byName.put(f.name(), f));
        return new FieldCapabilitiesIndexResponse(name, mappingHash, byName, true, IndexMode.STANDARD, 1, 1L, 1L);
    }

    private static EsIndex resolve(boolean trackUnmappedFieldIndices, FieldCapabilitiesIndexResponse... indices) {
        return resolve(true, trackUnmappedFieldIndices, indices);
    }

    private static EsIndex resolve(
        boolean reportNonNullable,
        boolean trackUnmappedFieldIndices,
        FieldCapabilitiesIndexResponse... indices
    ) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indices)).build();
        IndexResolution resolution = IndexResolver.mergedMappings(
            "idx-*",
            false,
            new IndexResolver.FieldsInfo(caps, TransportVersion.current(), false, false, false, false, false, false, reportNonNullable),
            trackUnmappedFieldIndices,
            IndexResolver.DO_NOT_GROUP
        );
        return resolution.get();
    }
}
