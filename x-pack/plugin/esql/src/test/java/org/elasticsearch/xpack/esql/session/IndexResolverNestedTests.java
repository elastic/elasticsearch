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
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedReduction;
import org.elasticsearch.xpack.esql.index.NestedField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * A field under a nested object is a column only where every node reads it flattened; field caps no longer leaves it out.
 */
public class IndexResolverNestedTests extends ESTestCase {

    public void testFieldUnderNestedObjectIsAColumn() {
        assumeTrue("snapshot builds read the fields under a nested object", NestedField.FLATTENED_LEAVES);
        Map<String, EsField> mapping = resolve(TransportVersion.current());
        EsField pid = mapping.get("process").getProperties().get("pid");
        assertThat(pid.getDataType(), equalTo(DataType.LONG));
        assertThat(pid.isAggregatable(), equalTo(false));
        assertThat(mapping.get("name").getDataType(), equalTo(DataType.KEYWORD));
    }

    /**
     * An older data node can't read the children, so it would load null for a column a newer one fills. Every coordinator
     * leaves the field out, as field caps did before.
     */
    public void testWithAnOlderNodeFieldUnderNestedObjectIsLeftOut() {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(NestedReduction.VERSION);
        Map<String, EsField> mapping = resolve(older);
        assertThat(mapping.get("process"), nullValue());
        assertThat(mapping.get("name").getDataType(), equalTo(DataType.KEYWORD));
    }

    private static Map<String, EsField> resolve(TransportVersion minimumVersion) {
        FieldCapabilitiesIndexResponse index = index(
            "test",
            new IndexFieldCapabilitiesBuilder("process", "nested").build(),
            new IndexFieldCapabilitiesBuilder("process.pid", "long").build(),
            new IndexFieldCapabilitiesBuilder("name", "keyword").build()
        );
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(index)).build();
        return IndexResolver.mergedMappings(
            "*",
            false,
            new IndexResolver.FieldsInfo(caps, minimumVersion, false, false, false, false, false, false),
            false,
            IndexResolver.DO_NOT_GROUP
        ).get().mapping();
    }

    private static FieldCapabilitiesIndexResponse index(String index, IndexFieldCapabilities... fields) {
        Map<String, IndexFieldCapabilities> byName = new HashMap<>();
        for (IndexFieldCapabilities field : fields) {
            byName.put(field.name(), field);
        }
        return new FieldCapabilitiesIndexResponse(index, index, byName, true, IndexMode.STANDARD, 1, 0, 1L, 1L);
    }
}
