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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The nested fields of a mapping are read into their private schema only for a query that asked field caps for them. Without that
 * request field caps has already left them out, so merging the mappings neither looks for them nor builds the schema.
 */
public class IndexResolverNestedScopeTests extends ESTestCase {

    public void testNestedFieldsGoToThePrivateSchemaWhenNestedScopeIsSet() {
        Map<String, EsField> mapping = resolve(true, nestedIndex("ne"));
        assertThat(mapping, hasKey(NestedScopeField.NAMESPACE));
        assertThat(mapping.get(NestedScopeField.NAMESPACE).getProperties().get("items"), instanceOf(NestedScopeField.class));
        assertThat(mapping, hasKey("id"));
        assertThat(mapping, not(hasKey("items")));
    }

    public void testNestedFieldsAreNotScannedWithoutNestedScope() {
        Map<String, EsField> mapping = resolve(false, nestedIndex("ne"));
        assertThat(mapping, not(hasKey(NestedScopeField.NAMESPACE)));
        assertThat(mapping, hasKey("id"));
    }

    public void testOrdinaryFieldsResolveTheSameWhateverTheNestedScope() {
        FieldCapabilitiesIndexResponse[] indices = {
            index("one", field("id", "keyword"), field("obj", "object"), field("obj.n", "long"), field("when", "date")),
            index("two", field("id", "keyword"), field("obj", "object"), field("obj.n", "long"), field("when", "date")) };
        Map<String, EsField> withScope = resolve(true, indices);
        Map<String, EsField> withoutScope = resolve(false, indices);
        assertThat(withoutScope, equalTo(withScope));
        assertThat(withoutScope, not(hasKey(NestedScopeField.NAMESPACE)));
        assertThat(withoutScope.get("obj").getProperties(), aMapWithSize(1));
    }

    public void testTypeConflictsResolveTheSameWhateverTheNestedScope() {
        FieldCapabilitiesIndexResponse[] indices = {
            index("one", field("id", "keyword"), field("x", "keyword")),
            index("two", field("id", "keyword"), field("x", "long")) };
        Map<String, EsField> withScope = resolve(true, indices);
        Map<String, EsField> withoutScope = resolve(false, indices);
        assertThat(withoutScope, equalTo(withScope));
        assertThat(withoutScope.get("x").getDataType(), equalTo(withScope.get("x").getDataType()));
    }

    public void testNestedLeafFollowsTheFlattenedKillSwitchLikeAnOrdinaryField() {
        FieldCapabilitiesIndexResponse index = index(
            "ne",
            field("attrs", "flattened"),
            field("items", "nested"),
            field("items.attrs", "flattened")
        );
        Map<String, EsField> mapping = resolve(true, TransportVersion.current(), false, index);
        assertThat(mapping.get("attrs").getDataType(), equalTo(DataType.UNSUPPORTED));
        assertThat(nestedLeaf(mapping, "items", "attrs").getDataType(), equalTo(DataType.UNSUPPORTED));
        Map<String, EsField> enabled = resolve(true, TransportVersion.current(), true, index);
        assertThat(enabled.get("attrs").getDataType(), equalTo(DataType.FLATTENED));
        assertThat(nestedLeaf(enabled, "items", "attrs").getDataType(), equalTo(DataType.FLATTENED));
    }

    public void testNestedLeafFollowsTheMinimumTransportVersionLikeAnOrdinaryField() {
        FieldCapabilitiesIndexResponse index = index(
            "ne",
            field("span", "date_range"),
            field("items", "nested"),
            field("items.span", "date_range")
        );
        Map<String, EsField> mapping = resolve(true, TransportVersion.minimumCompatible(), true, index);
        assertThat(mapping.get("span").getDataType(), equalTo(DataType.UNSUPPORTED));
        assertThat(nestedLeaf(mapping, "items", "span").getDataType(), equalTo(DataType.UNSUPPORTED));
    }

    public void testNestedLeafIsTheFieldAnOrdinaryFieldWouldBe() {
        String[][] typesPerIndex = {
            { "keyword" },
            { "long" },
            { "date" },
            { "text" },
            { "short" },
            { "completion" },
            { "keyword", "keyword" },
            { "integer", "keyword" },
            { "short", "integer" },
            { "long", "double" },
            { "text", "keyword" } };
        for (String[] types : typesPerIndex) {
            FieldCapabilitiesIndexResponse[] indices = new FieldCapabilitiesIndexResponse[types.length];
            for (int i = 0; i < types.length; i++) {
                indices[i] = index("i" + i, field("v", types[i]), field("items", "nested"), field("items.v", types[i]));
            }
            Map<String, EsField> mapping = resolve(true, indices);
            NestedScopeField nested = (NestedScopeField) nestedLeaf(mapping, "items", "v");
            assertThat(Arrays.toString(types), nested.leaf(), equalTo(mapping.get("v")));
        }
    }

    public void testNestedLeafMergesOnlyTheOccurrencesInNestedScope() {
        Map<String, EsField> mapping = resolve(
            true,
            index("a", field("items", "nested"), field("items.v", "integer")),
            index("b", field("items", "nested"), field("items.v", "keyword")),
            index("c", field("items", "object"), field("items.v", "long"))
        );
        TypeConflictedField leaf = (TypeConflictedField) ((NestedScopeField) nestedLeaf(mapping, "items", "v")).leaf();
        assertThat(leaf.getTypesToIndices(), equalTo(Map.of("integer", Set.of("a"), "keyword", Set.of("b"))));
        assertThat(mapping.get("items").getProperties().get("v").getDataType(), equalTo(DataType.LONG));
    }

    public void testSmallNumericLeafIsWidenedLikeAnOrdinaryColumn() {
        Map<String, EsField> mapping = resolve(
            true,
            index("a", field("small", "short"), field("items", "nested"), field("items.small", "short"))
        );
        NestedScopeField nested = (NestedScopeField) nestedLeaf(mapping, "items", "small");
        assertThat(nested.getDataType(), equalTo(DataType.SHORT));
        assertThat(nested.withWidenedSmallNumeric(), instanceOf(NestedScopeField.class));
        assertThat(nested.withWidenedSmallNumeric().getDataType(), equalTo(DataType.INTEGER));
        assertThat(((NestedScopeField) nested.withWidenedSmallNumeric()).leaf().getDataType(), equalTo(DataType.INTEGER));
    }

    private static EsField nestedLeaf(Map<String, EsField> mapping, String collection, String name) {
        return mapping.get(NestedScopeField.NAMESPACE).getProperties().get(collection).getProperties().get(name);
    }

    private static FieldCapabilitiesIndexResponse nestedIndex(String name) {
        return index(name, field("id", "keyword"), field("items", "nested"), field("items.sku", "keyword"), field("items.qty", "long"));
    }

    private static Map<String, EsField> resolve(boolean nestedScope, FieldCapabilitiesIndexResponse... indices) {
        return resolve(nestedScope, TransportVersion.current(), true, indices);
    }

    private static Map<String, EsField> resolve(
        boolean nestedScope,
        TransportVersion minTransportVersion,
        boolean flattenedDataTypeEnabled,
        FieldCapabilitiesIndexResponse... indices
    ) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indices)).build();
        return IndexResolver.mergedMappings(
            "*",
            false,
            new IndexResolver.FieldsInfo(
                caps,
                minTransportVersion,
                false,
                false,
                false,
                false,
                false,
                false,
                flattenedDataTypeEnabled,
                null,
                nestedScope
            ),
            false,
            IndexResolver.DO_NOT_GROUP
        ).get().mapping();
    }

    private static IndexFieldCapabilities field(String name, String type) {
        return new IndexFieldCapabilitiesBuilder(name, type).build();
    }

    private static FieldCapabilitiesIndexResponse index(String index, IndexFieldCapabilities... fields) {
        Map<String, IndexFieldCapabilities> byName = new HashMap<>();
        for (IndexFieldCapabilities field : fields) {
            byName.put(field.name(), field);
        }
        return new FieldCapabilitiesIndexResponse(index, index, byName, true, IndexMode.STANDARD, 1, 0, 1L, 1L, null);
    }
}
