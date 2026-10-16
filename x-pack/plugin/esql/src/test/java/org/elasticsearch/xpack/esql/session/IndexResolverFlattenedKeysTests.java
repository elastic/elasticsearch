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
import static org.hamcrest.Matchers.not;

/**
 * A name another index maps is a {@code keyword} in the indices where it is under a {@code flattened} field.
 */
public class IndexResolverFlattenedKeysTests extends ESTestCase {

    public void testKeyAgreesWithAKeyword() {
        List<FieldCapabilitiesIndexResponse> indices = List.of(
            index("flat", field("attrs", "flattened")),
            index("kw", field("attrs", "object"), field("attrs.bar", "keyword"))
        );
        // Mapped by every index, so it is not a field that may be unmapped
        EsField bar = attrs(resolve(indices, true, true)).get("bar");
        assertThat(bar.getDataType(), equalTo(DataType.KEYWORD));
        assertThat(bar, not(instanceOf(PotentiallyUnmappedKeywordEsField.class)));
        // Without the keys, the flattened index does not map it
        assertThat(attrs(resolve(indices, false, true)).get("bar"), instanceOf(PotentiallyUnmappedKeywordEsField.class));
    }

    public void testKeyConflictsWithAnotherType() {
        List<FieldCapabilitiesIndexResponse> indices = List.of(
            index("flat", field("attrs", "flattened")),
            index("dt", field("attrs", "object"), field("attrs.time", "date"))
        );
        EsField time = attrs(resolve(indices, true, false)).get("time");
        assertThat(time, instanceOf(TypeConflictedField.class));
        assertThat(((TypeConflictedField) time).getTypesToIndices(), equalTo(Map.of("keyword", Set.of("flat"), "datetime", Set.of("dt"))));
        assertThat(attrs(resolve(indices, false, false)).get("time").getDataType(), equalTo(DataType.DATETIME));
    }

    public void testAnObjectBesideAFlattenedFieldAddsNothing() {
        List<FieldCapabilitiesIndexResponse> indices = List.of(
            index("flat", field("attrs", "flattened")),
            index("kw", field("attrs", "object"), field("attrs.bar", "keyword"))
        );
        assertThat(resolve(indices, true, false).get("attrs").getDataType(), equalTo(DataType.FLATTENED));
        // Without the keys, a flattened field beside an object is a conflict
        assertThat(resolve(indices, false, false).get("attrs").getDataType(), equalTo(DataType.UNSUPPORTED));
    }

    public void testAnObjectIsNotAKey() {
        List<FieldCapabilitiesIndexResponse> indices = List.of(
            index("flat", field("attrs", "flattened")),
            index("obj", field("attrs", "object"), field("attrs.o", "object"), field("attrs.o.leaf", "keyword"))
        );
        Map<String, EsField> attrs = attrs(resolve(indices, true, false));
        assertThat(attrs.get("o").getDataType(), equalTo(DataType.OBJECT));
        assertThat(attrs.get("o").getProperties().get("leaf").getDataType(), equalTo(DataType.KEYWORD));
    }

    public void testIndexWithoutAFlattenedFieldHasNoKeys() {
        List<FieldCapabilitiesIndexResponse> indices = List.of(
            index("other", field("other", "keyword")),
            index("kw", field("attrs", "object"), field("attrs.bar", "keyword"))
        );
        assertThat(attrs(resolve(indices, true, true)).get("bar"), instanceOf(PotentiallyUnmappedKeywordEsField.class));
    }

    public void testOlderNodesHaveNoKeys() {
        List<FieldCapabilitiesIndexResponse> indices = List.of(
            index("flat", field("attrs", "flattened")),
            index("dt", field("attrs", "object"), field("attrs.time", "date"))
        );
        TransportVersion before = TransportVersionUtils.getPreviousVersion(Configuration.ESQL_SHARD_RESOLVED_FIELDS);
        assertThat(attrs(resolve(indices, true, false, before)).get("time").getDataType(), equalTo(DataType.DATETIME));
    }

    private static Map<String, EsField> attrs(Map<String, EsField> mapping) {
        return mapping.get("attrs").getProperties();
    }

    private static Map<String, EsField> resolve(List<FieldCapabilitiesIndexResponse> indices, boolean flattenedKeys, boolean track) {
        return resolve(indices, flattenedKeys, track, TransportVersion.current());
    }

    private static Map<String, EsField> resolve(
        List<FieldCapabilitiesIndexResponse> indices,
        boolean flattenedKeys,
        boolean track,
        TransportVersion minimumVersion
    ) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(indices).build();
        var fieldsInfo = new IndexResolver.FieldsInfo(
            caps,
            minimumVersion,
            false,
            false,
            false,
            false,
            false,
            true,
            flattenedKeys ? Set.of() : null
        );
        return IndexResolver.mergedMappings("*", false, fieldsInfo, track, IndexResolver.DO_NOT_GROUP).get().mapping();
    }

    private static IndexFieldCapabilities field(String name, String type) {
        return new IndexFieldCapabilitiesBuilder(name, type).build();
    }

    private static FieldCapabilitiesIndexResponse index(String index, IndexFieldCapabilities... fields) {
        Map<String, IndexFieldCapabilities> byName = new HashMap<>();
        for (IndexFieldCapabilities field : fields) {
            byName.put(field.name(), field);
        }
        return new FieldCapabilitiesIndexResponse(index, index, byName, true, IndexMode.STANDARD, 1, 1L, 1L);
    }
}
