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
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * An object is not a leaf: it never gives a column its type.
 */
public class IndexResolverObjectTests extends ESTestCase {

    public void testAnObjectBesideAField() {
        Map<String, EsField> mapping = resolve(
            index("field", field("a", "keyword")),
            index("object", field("a", "object"), field("a.b", "long"))
        );
        assertThat(mapping.get("a").getDataType(), equalTo(DataType.KEYWORD));
        assertThat(mapping.get("a").getProperties().get("b").getDataType(), equalTo(DataType.LONG));
    }

    public void testAnObjectBesideConflictingFields() {
        Map<String, EsField> mapping = resolve(
            index("keyword", field("a", "keyword")),
            index("long", field("a", "long")),
            index("object", field("a", "object"), field("a.b", "long"))
        );
        assertThat(mapping.get("a"), instanceOf(TypeConflictedField.class));
        assertThat(((TypeConflictedField) mapping.get("a")).types(), equalTo(Set.of(DataType.KEYWORD, DataType.LONG)));
    }

    /**
     * Where the query loads unmapped fields, each shard of the index of the object tells whether it holds a value.
     */
    public void testAnObjectDoesNotMapItsName() {
        Map<String, EsField> mapping = resolve(
            TransportVersion.current(),
            true,
            index("field", field("a", "keyword")),
            index("object", field("a", "object"), field("a.b", "long"))
        );
        assertThat(mapping.get("a"), instanceOf(PotentiallyUnmappedKeywordEsField.class));
    }

    /**
     * With a node that predates it, an object is a leaf as before: every coordinator resolves the column alike, and no data node
     * reads {@code _source} for it.
     */
    public void testWithAnOlderNodeAnObjectIsALeaf() {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_objects_are_not_leaves"));
        Map<String, EsField> mapping = resolve(
            older,
            true,
            index("field", field("a", "keyword")),
            index("object", field("a", "object"), field("a.b", "long"))
        );
        assertThat(((TypeConflictedField) mapping.get("a")).types(), equalTo(Set.of(DataType.KEYWORD, DataType.OBJECT)));
    }

    public void testAnObjectOnly() {
        Map<String, EsField> mapping = resolve(index("object", field("a", "object"), field("a.b", "long")));
        assertThat(mapping.get("a").getDataType(), equalTo(DataType.OBJECT));
        assertThat(mapping.get("a").getProperties().get("b").getDataType(), equalTo(DataType.LONG));
    }

    private static Map<String, EsField> resolve(FieldCapabilitiesIndexResponse... indices) {
        return resolve(TransportVersion.current(), false, indices);
    }

    private static Map<String, EsField> resolve(
        TransportVersion minimumVersion,
        boolean trackUnmappedFieldIndices,
        FieldCapabilitiesIndexResponse... indices
    ) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indices)).build();
        return IndexResolver.mergedMappings(
            "*",
            false,
            new IndexResolver.FieldsInfo(caps, minimumVersion, false, false, false, false, false, false),
            trackUnmappedFieldIndices,
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
        return new FieldCapabilitiesIndexResponse(index, index, byName, true, IndexMode.STANDARD, 1, 0, 1L, 1L);
    }
}
