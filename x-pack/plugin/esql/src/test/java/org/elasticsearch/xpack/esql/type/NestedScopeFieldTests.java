/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NestedScopeFieldTests extends ESTestCase {

    private static final Function<IndexFieldCapabilities, DataType> ORDINARY_TYPES = fc -> EsqlDataTypeRegistry.INSTANCE.fromEs(
        fc.type(),
        fc.metricType()
    );

    public void testNoNestedFieldHasNoNamespace() {
        assertThat(
            NestedScopeField.namespace(List.of(index("a", field("id", "keyword"), field("obj", "object"))), ORDINARY_TYPES),
            nullValue()
        );
    }

    public void testIndicesThatAgreeShareOneCollection() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("items", "nested"), field("items.sku", "keyword")),
                index("b", field("items", "nested"), field("items.sku", "keyword"), field("items.qty", "long"))
            ),
            ORDINARY_TYPES
        );
        NestedScopeField items = scoped(root, "items");
        assertThat(items.kind(), equalTo(NestedScopeField.Kind.COLLECTION));
        assertThat(items.path(), equalTo("items"));
        assertThat(items.conflict(), nullValue());
        assertThat(scoped(items, "sku").kind(), equalTo(NestedScopeField.Kind.VALUE));
        assertThat(scoped(items, "sku").getDataType(), equalTo(DataType.KEYWORD));
        assertThat(scoped(items, "qty").getDataType(), equalTo(DataType.LONG));
    }

    public void testLeavesOfDifferentTypesAreUnsupported() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("items", "nested"), field("items.v", "keyword")),
                index("b", field("items", "nested"), field("items.v", "long"))
            ),
            ORDINARY_TYPES
        );
        assertThat(scoped(scoped(root, "items"), "v").getDataType(), equalTo(DataType.UNSUPPORTED));
    }

    public void testSmallNumericsWidenAndDoNotConflict() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("items", "nested"), field("items.n", "short"), field("items.f", "float"), field("items.b", "byte")),
                index("b", field("items", "nested"), field("items.n", "integer"), field("items.f", "double"), field("items.b", "byte"))
            ),
            ORDINARY_TYPES
        );
        NestedScopeField items = scoped(root, "items");
        assertThat(scoped(items, "n").getDataType(), equalTo(DataType.INTEGER));
        assertThat(scoped(items, "f").getDataType(), equalTo(DataType.DOUBLE));
        assertThat(scoped(items, "b").getDataType(), equalTo(DataType.INTEGER));
    }

    public void testUnknownTypeIsUnsupported() {
        EsField root = NestedScopeField.namespace(
            List.of(index("a", field("items", "nested"), field("items.h", "completion"))),
            ORDINARY_TYPES
        );
        assertThat(scoped(scoped(root, "items"), "h").getDataType(), equalTo(DataType.UNSUPPORTED));
    }

    public void testObjectUnderACollectionIsNotAValue() {
        EsField root = NestedScopeField.namespace(
            List.of(index("a", field("items", "nested"), field("items.o", "object"), field("items.o.v", "keyword"))),
            ORDINARY_TYPES
        );
        NestedScopeField o = scoped(scoped(root, "items"), "o");
        assertThat(o.kind(), equalTo(NestedScopeField.Kind.OBJECT));
        assertThat(scoped(o, "v").kind(), equalTo(NestedScopeField.Kind.VALUE));
        assertThat(scoped(o, "v").path(), equalTo("items"));
    }

    public void testPathDisagreementWinsOverANestedMappingConflict() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("p", "nested"), field("p.q", "nested"), field("p.q.v", "keyword")),
                index("b", field("p", "nested"), field("p.q", "object"), field("p.q.v", "keyword")),
                index("c", field("p", "nested"), field("p.q", "keyword"), field("p.q.v", "keyword"))
            ),
            ORDINARY_TYPES
        );
        NestedScopeField v = scoped(scoped(scoped(root, "p"), "q"), "v");
        assertThat(v.conflict(), equalTo("its nested path is [p.q] in [a] but [p] in [b]"));
    }

    public void testNestedInOneIndexAndObjectInAnotherConflictsForEveryChild() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("items", "nested"), field("items.sku", "keyword"), field("items.qty", "long")),
                index("b", field("items", "object"), field("items.sku", "keyword"), field("items.qty", "long"))
            ),
            ORDINARY_TYPES
        );
        String conflict = "[items] is [nested] in [a] but [object] in [b]";
        NestedScopeField items = scoped(root, "items");
        assertThat(items.conflict(), equalTo(conflict));
        assertThat(scoped(items, "sku").conflict(), equalTo(conflict));
        assertThat(scoped(items, "qty").conflict(), equalTo(conflict));
    }

    public void testObjectIsReportedFromTheFirstIndexByName() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("c", field("items", "object"), field("items.sku", "keyword")),
                index("b", field("items", "flattened"), field("items.sku", "keyword")),
                index("a", field("items", "nested"), field("items.sku", "keyword"))
            ),
            ORDINARY_TYPES
        );
        assertThat(scoped(root, "items").conflict(), equalTo("[items] is [nested] in [a] but [flattened] in [b]"));
    }

    public void testChildUnderDifferentNestedPathsConflicts() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("p", "nested"), field("p.q", "nested"), field("p.q.v", "keyword")),
                index("b", field("p", "nested"), field("p.q", "object"), field("p.q.v", "keyword"))
            ),
            ORDINARY_TYPES
        );
        NestedScopeField q = scoped(scoped(root, "p"), "q");
        assertThat(scoped(q, "v").conflict(), equalTo("its nested path is [p.q] in [a] but [p] in [b]"));
    }

    public void testScopeOfIdenticalMappingsIsBuiltOnce() {
        for (int n : new int[] { 1, 50 }) {
            AtomicLong lookups = new AtomicLong();
            List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                responses.add(
                    new FieldCapabilitiesIndexResponse(
                        "idx" + i,
                        "same-mapping",
                        counting(lookups, field("items", "nested"), field("items.sku", "keyword")),
                        true,
                        IndexMode.STANDARD,
                        1,
                        0,
                        1L,
                        1L
                    )
                );
            }
            assertThat(NestedScopeField.namespace(responses, ORDINARY_TYPES), notNullValue());
            assertThat("indices: " + n, lookups.get(), lessThanOrEqualTo(10L));
        }
    }

    public void testWorkGrowsLinearlyWithTheNumberOfIndices() {
        for (int n : new int[] { 500, 2000 }) {
            AtomicLong lookups = new AtomicLong();
            List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                responses.add(
                    new FieldCapabilitiesIndexResponse(
                        "idx" + i,
                        "mapping" + i,
                        counting(lookups, field("p" + i, "nested"), field("p" + i + ".v", "keyword")),
                        true,
                        IndexMode.STANDARD,
                        1,
                        0,
                        1L,
                        1L
                    )
                );
            }
            assertThat(NestedScopeField.namespace(responses, ORDINARY_TYPES), notNullValue());
            assertThat("indices: " + n, lookups.get(), lessThanOrEqualTo(10L * n));
        }
    }

    private static NestedScopeField scoped(EsField parent, String name) {
        return (NestedScopeField) parent.getProperties().get(name);
    }

    private static IndexFieldCapabilities field(String name, String type) {
        return new IndexFieldCapabilitiesBuilder(name, type).build();
    }

    private static FieldCapabilitiesIndexResponse index(String name, IndexFieldCapabilities... fields) {
        Map<String, IndexFieldCapabilities> byName = new HashMap<>();
        for (IndexFieldCapabilities field : fields) {
            byName.put(field.name(), field);
        }
        return new FieldCapabilitiesIndexResponse(name, name, byName, true, IndexMode.STANDARD, 1, 0, 1L, 1L);
    }

    private static Map<String, IndexFieldCapabilities> counting(AtomicLong lookups, IndexFieldCapabilities... fields) {
        Map<String, IndexFieldCapabilities> byName = new HashMap<>() {
            @Override
            public IndexFieldCapabilities get(Object key) {
                lookups.incrementAndGet();
                return super.get(key);
            }
        };
        for (IndexFieldCapabilities field : fields) {
            byName.put(field.name(), field);
        }
        return byName;
    }
}
