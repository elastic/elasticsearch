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
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NestedScopeFieldTests extends ESTestCase {

    /** Stands in for ordinary resolution: the type of the first occurrence, with no merging of types, which is its own tests' job. */
    private static final NestedScopeField.LeafMerger FIRST_OCCURRENCE = (name, fullName, occurrences) -> new EsField(
        name,
        EsqlDataTypeRegistry.INSTANCE.fromEs(occurrences.getFirst().type(), occurrences.getFirst().metricType()),
        new TreeMap<>(),
        false,
        EsField.TimeSeriesFieldType.NONE
    );

    public void testNoNestedFieldHasNoNamespace() {
        assertThat(
            NestedScopeField.namespace(List.of(index("a", field("id", "keyword"), field("obj", "object"))), FIRST_OCCURRENCE),
            nullValue()
        );
    }

    public void testAKeyOfAFlattenedFieldIsAChildOfItsCollection() {
        EsField root = NestedScopeField.namespace(
            List.of(index("a", field("items", "nested"), field("items.attrs", "flattened"), field("other", "nested"))),
            FIRST_OCCURRENCE,
            Set.of("items.attrs.color"),
            (name, fields) -> fields.containsKey("items.attrs") ? field(name, "keyword") : null
        );
        NestedScopeField color = scoped(scoped(scoped(root, "items"), "attrs"), "color");
        assertThat(color.getDataType(), equalTo(DataType.KEYWORD));
        assertThat(color.path(), equalTo("items"));
        assertThat(scoped(root, "other").getProperties().isEmpty(), equalTo(true));
    }

    public void testAKeyIsNoChildWhereAnotherIndexMapsTheNameAsAField() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("items", "nested"), field("items.attrs", "object"), field("items.attrs.score", "long")),
                index("b", field("items", "nested"), field("items.attrs", "flattened"))
            ),
            FIRST_OCCURRENCE,
            Set.of("items.attrs.score"),
            (name, fields) -> fields.containsKey("items.attrs") ? field(name, "keyword") : null
        );
        NestedScopeField score = scoped(scoped(scoped(root, "items"), "attrs"), "score");
        assertThat(score.getDataType(), equalTo(DataType.LONG));
    }

    public void testAKeyIsNotAChildWhereNoIndexHoldsItInANestedFlattenedField() {
        EsField root = NestedScopeField.namespace(
            List.of(index("a", field("items", "nested"), field("items.sku", "keyword"))),
            FIRST_OCCURRENCE,
            Set.of("items.attrs.color"),
            (name, fields) -> null
        );
        assertThat(scoped(root, "items").getProperties().containsKey("attrs"), equalTo(false));
    }

    public void testIndicesThatAgreeShareOneCollection() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("items", "nested"), field("items.sku", "keyword")),
                index("b", field("items", "nested"), field("items.sku", "keyword"), field("items.qty", "long"))
            ),
            FIRST_OCCURRENCE
        );
        NestedScopeField items = scoped(root, "items");
        assertThat(items.kind(), equalTo(NestedScopeField.Kind.COLLECTION));
        assertThat(items.path(), equalTo("items"));
        assertThat(items.conflict(), nullValue());
        assertThat(scoped(items, "sku").kind(), equalTo(NestedScopeField.Kind.VALUE));
        assertThat(scoped(items, "sku").getDataType(), equalTo(DataType.KEYWORD));
        assertThat(scoped(items, "qty").getDataType(), equalTo(DataType.LONG));
    }

    public void testValueKeepsTheLeafTheMergerMade() {
        List<List<IndexFieldCapabilities>> seen = new ArrayList<>();
        NestedScopeField.LeafMerger recording = (name, fullName, occurrences) -> {
            seen.add(occurrences);
            return new EsField(name, DataType.KEYWORD, new TreeMap<>(), false, EsField.TimeSeriesFieldType.NONE);
        };
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("items", "nested"), field("items.sku", "keyword")),
                index("b", field("items", "nested"), field("items.sku", "keyword"))
            ),
            recording
        );
        NestedScopeField sku = scoped(scoped(root, "items"), "sku");
        assertThat(sku.leaf(), equalTo(new EsField("sku", DataType.KEYWORD, new TreeMap<>(), false, EsField.TimeSeriesFieldType.NONE)));
        assertThat(sku.getDataType(), equalTo(DataType.KEYWORD));
        assertThat(seen.size(), equalTo(1));
    }

    public void testOnlyValuesCarryALeaf() {
        EsField root = NestedScopeField.namespace(
            List.of(index("a", field("items", "nested"), field("items.o", "object"), field("items.o.v", "keyword"))),
            FIRST_OCCURRENCE
        );
        NestedScopeField items = scoped(root, "items");
        assertThat(items.leaf(), nullValue());
        assertThat(scoped(items, "o").leaf(), nullValue());
        assertThat(scoped(scoped(items, "o"), "v").leaf(), notNullValue());
    }

    public void testFieldNestedInOneIndexAndNotInAnotherIsAnUnsupportedValueWithALeaf() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("p", "nested"), field("p.q", "nested"), field("p.q.v", "keyword")),
                index("b", field("p", "nested"), field("p.q", "keyword"))
            ),
            FIRST_OCCURRENCE
        );
        NestedScopeField q = scoped(scoped(root, "p"), "q");
        assertThat(q.kind(), equalTo(NestedScopeField.Kind.VALUE));
        assertThat(q.getDataType(), equalTo(DataType.UNSUPPORTED));
        assertThat(q.leaf(), notNullValue());
        assertThat(q.leaf().getDataType(), equalTo(DataType.UNSUPPORTED));
    }

    public void testObjectUnderACollectionIsNotAValue() {
        EsField root = NestedScopeField.namespace(
            List.of(index("a", field("items", "nested"), field("items.o", "object"), field("items.o.v", "keyword"))),
            FIRST_OCCURRENCE
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
            FIRST_OCCURRENCE
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
            FIRST_OCCURRENCE
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
            FIRST_OCCURRENCE
        );
        assertThat(scoped(root, "items").conflict(), equalTo("[items] is [nested] in [a] but [flattened] in [b]"));
    }

    public void testChildUnderDifferentNestedPathsConflicts() {
        EsField root = NestedScopeField.namespace(
            List.of(
                index("a", field("p", "nested"), field("p.q", "nested"), field("p.q.v", "keyword")),
                index("b", field("p", "nested"), field("p.q", "object"), field("p.q.v", "keyword"))
            ),
            FIRST_OCCURRENCE
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
            assertThat(NestedScopeField.namespace(responses, FIRST_OCCURRENCE), notNullValue());
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
            assertThat(NestedScopeField.namespace(responses, FIRST_OCCURRENCE), notNullValue());
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
