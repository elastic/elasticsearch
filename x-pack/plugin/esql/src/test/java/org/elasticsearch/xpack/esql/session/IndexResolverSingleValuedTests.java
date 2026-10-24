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
 * Tests which fields {@link IndexResolver#mergedMappings} reports in {@link EsField#isStaticallySingleValued()}: a field qualifies only
 * if every index that has it guarantees one value per document and the field is a plain mapped field.
 */
public class IndexResolverSingleValuedTests extends ESTestCase {

    public void testFlaggedInEveryIndex() {
        EsIndex index = resolve(false, index("a", field("x", "long", true)), index("b", field("x", "long", true)));
        assertThat(singleValuedFields(index), equalTo(Set.of("x")));
    }

    public void testOneIndexWithoutTheGuaranteeDisqualifiesTheField() {
        EsIndex index = resolve(false, index("a", field("x", "long", true)), index("b", field("x", "long", false)));
        assertThat(singleValuedFields(index), empty());
    }

    public void testNoGuaranteeByDefault() {
        EsIndex index = resolve(false, index("a", field("x", "long", false)));
        assertThat(singleValuedFields(index), empty());
    }

    public void testFieldMissingFromAnIndexStillQualifies() {
        EsIndex index = resolve(false, index("a", field("x", "long", true)), index("b", field("y", "long", true)));
        assertThat(singleValuedFields(index), equalTo(Set.of("x", "y")));
    }

    public void testKeywordAndDateQualify() {
        EsIndex index = resolve(false, index("a", field("k", "keyword", true), field("d", "date", true)));
        assertThat(singleValuedFields(index), equalTo(Set.of("k", "d")));
    }

    public void testDottedFieldUsesFullName() {
        EsIndex index = resolve(false, index("a", field("outer.inner", "long", true)));
        assertThat(singleValuedFields(index), equalTo(Set.of("outer.inner")));
    }

    public void testTypeConflictIsNotSingleValued() {
        EsIndex index = resolve(false, index("a", field("x", "long", true)), index("b", field("x", "keyword", true)));
        assertThat(singleValuedFields(index), empty());
    }

    public void testTextIsNotSingleValued() {
        EsIndex index = resolve(false, index("a", field("t", "text", true)));
        assertThat(singleValuedFields(index), empty());
    }

    /**
     * With unmapped field tracking, a field that one index does not map may be read from {@code _source} there, where nothing
     * limits it to one value.
     */
    public void testPartiallyUnmappedFieldIsNotSingleValued() {
        EsIndex index = resolve(true, index("a", field("x", "long", true)), index("b", field("y", "long", true)));
        assertThat(singleValuedFields(index), empty());
    }

    /** The full names of the fields that carry the guarantee. */
    private static Set<String> singleValuedFields(EsIndex index) {
        Set<String> names = new HashSet<>();
        collectSingleValued("", index.mapping(), names);
        return names;
    }

    private static void collectSingleValued(String prefix, Map<String, EsField> fields, Set<String> names) {
        fields.forEach((name, field) -> {
            String fullName = prefix + name;
            if (field.isStaticallySingleValued()) {
                names.add(fullName);
            }
            collectSingleValued(fullName + ".", field.getProperties(), names);
        });
    }

    private static IndexFieldCapabilities field(String name, String type, boolean singleValued) {
        return new IndexFieldCapabilitiesBuilder(name, type).isStaticallySingleValued(singleValued).build();
    }

    private static FieldCapabilitiesIndexResponse index(String name, IndexFieldCapabilities... fields) {
        Map<String, IndexFieldCapabilities> byName = new HashMap<>();
        Arrays.stream(fields).forEach(f -> byName.put(f.name(), f));
        return new FieldCapabilitiesIndexResponse(name, name, byName, true, IndexMode.STANDARD, 1, 1L, 1L);
    }

    private static EsIndex resolve(boolean trackUnmappedFieldIndices, FieldCapabilitiesIndexResponse... indices) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indices)).build();
        IndexResolution resolution = IndexResolver.mergedMappings(
            "idx-*",
            false,
            new IndexResolver.FieldsInfo(caps, TransportVersion.current(), false, false, false, false, false, false),
            trackUnmappedFieldIndices,
            IndexResolver.DO_NOT_GROUP
        );
        return resolution.get();
    }
}
