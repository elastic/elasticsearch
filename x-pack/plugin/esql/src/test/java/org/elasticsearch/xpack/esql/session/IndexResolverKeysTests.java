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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * A name the query uses under a {@code flattened} field is the {@code keyword} key of that name, unless an index maps it as a leaf.
 */
public class IndexResolverKeysTests extends ESTestCase {

    public void testAKeyIsAKeyword() {
        Map<String, EsField> mapping = resolve(TransportVersion.current(), index("flat", field("attrs", "flattened")));
        assertThat(mapping.get("attrs").getDataType(), equalTo(DataType.FLATTENED));
        assertThat(mapping.get("attrs").getProperties().get("bar").getDataType(), equalTo(DataType.KEYWORD));
    }

    /** A key declares no type: the field another index maps gives the column its type. */
    public void testAMappedLeafTypesTheColumn() {
        Map<String, EsField> mapping = resolve(
            TransportVersion.current(),
            index("flat", field("attrs", "flattened")),
            index("typed", field("attrs", "object"), field("attrs.bar", "long"))
        );
        assertThat(mapping.get("attrs").getProperties().get("bar").getDataType(), equalTo(DataType.LONG));
    }

    public void testANameUnderNoFlattenedFieldIsNotAKey() {
        Map<String, EsField> mapping = resolve(TransportVersion.current(), index("kw", field("attrs", "keyword")));
        assertThat(mapping.get("attrs").getProperties().containsKey("bar"), equalTo(false));
    }

    /** With a node that predates keys, a name under a flattened field is not a field: no node reads it. */
    public void testWithAnOlderNodeANameUnderAFlattenedFieldIsNotAField() {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_flattened_keys"));
        Map<String, EsField> mapping = resolve(older, index("flat", field("attrs", "flattened")));
        assertThat(mapping.get("attrs").getProperties().containsKey("bar"), equalTo(false));
    }

    /**
     * A field returned only as the parent path of a name the query uses counts only if it holds keys, so it does not change how the
     * name resolves.
     */
    public void testAParentPathThatHoldsNoKeysIsNotAField() {
        FieldCapabilitiesIndexResponse index = index("ranges", field("f", "ip_range"), field("f.raw", "keyword"));
        Map<String, EsField> mapping = resolve(new IndexResolver.NamedFields(Set.of("f.raw"), Set.of("f")), index);
        assertThat(mapping.get("f").getDataType(), equalTo(DataType.OBJECT));
        assertThat(mapping.get("f").getProperties().get("raw").getDataType(), equalTo(DataType.KEYWORD));
    }

    /** An object returned only as a parent path still holds the names below it, so it stays, as a passthrough object does. */
    public void testAParentPathThatIsAnObjectStays() {
        Map<String, EsField> mapping = resolve(
            new IndexResolver.NamedFields(Set.of("metrics.cpu"), Set.of("metrics")),
            index("tsdb", field("metrics", "object"), field("metrics.cpu", "double"))
        );
        assertThat(mapping.get("metrics").getDataType(), equalTo(DataType.OBJECT));
        assertThat(mapping.get("metrics").getProperties().get("cpu").getDataType(), equalTo(DataType.DOUBLE));
    }

    public void testAParentPathThatHoldsKeysIsAField() {
        Map<String, EsField> mapping = resolve(
            new IndexResolver.NamedFields(Set.of("attrs.bar"), Set.of("attrs")),
            index("flat", field("attrs", "flattened"))
        );
        assertThat(mapping.get("attrs").getDataType(), equalTo(DataType.FLATTENED));
        assertThat(mapping.get("attrs").getProperties().get("bar").getDataType(), equalTo(DataType.KEYWORD));
    }

    private static Map<String, EsField> resolve(TransportVersion minimumVersion, FieldCapabilitiesIndexResponse... indices) {
        return resolve(minimumVersion, new IndexResolver.NamedFields(Set.of("attrs.bar"), Set.of()), indices);
    }

    private static Map<String, EsField> resolve(IndexResolver.NamedFields namedFields, FieldCapabilitiesIndexResponse... indices) {
        return resolve(TransportVersion.current(), namedFields, indices);
    }

    private static Map<String, EsField> resolve(
        TransportVersion minimumVersion,
        IndexResolver.NamedFields namedFields,
        FieldCapabilitiesIndexResponse... indices
    ) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indices)).build();
        return IndexResolver.mergedMappings(
            "*",
            false,
            new IndexResolver.FieldsInfo(caps, minimumVersion, false, false, false, false, false, false, true, namedFields),
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
        return new FieldCapabilitiesIndexResponse(index, index, byName, true, IndexMode.STANDARD, 1, 0, 1L, 1L);
    }
}
