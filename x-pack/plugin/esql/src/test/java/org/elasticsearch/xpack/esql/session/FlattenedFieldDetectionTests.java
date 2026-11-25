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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Whether a resolution has a {@code flattened} field decides if the branches of views are kept through analysis, so that the keys of a
 * flattened column of a view can be exposed. A field that is {@code flattened} in some index counts, however the indices disagree.
 */
public class FlattenedFieldDetectionTests extends ESTestCase {

    public void testAFlattenedField() {
        assertThat(detects(index("a", field("attrs", "flattened"))), equalTo(true));
    }

    public void testAFlattenedFieldUnderAnObject() {
        assertThat(detects(index("a", field("meta", "object"), field("meta.labels", "flattened"))), equalTo(true));
    }

    public void testAFlattenedFieldThatAnotherIndexLacks() {
        assertThat(detects(index("a", field("attrs", "flattened")), index("b", field("id", "keyword"))), equalTo(true));
    }

    /** Flattened in one index and a keyword in another: a conflict, in whichever way the resolver represents it. */
    public void testAFlattenedFieldInConflictWithAnotherType() {
        assertThat(detects(index("a", field("attrs", "flattened")), index("b", field("attrs", "keyword"))), equalTo(true));
    }

    public void testNoFlattenedField() {
        assertThat(detects(index("a", field("attrs", "keyword"), field("id", "keyword"))), equalTo(false));
    }

    public void testAConflictThatIsNotAboutFlattened() {
        assertThat(detects(index("a", field("n", "long")), index("b", field("n", "keyword"))), equalTo(false));
    }

    private static boolean detects(FieldCapabilitiesIndexResponse... indices) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indices)).build();
        Map<String, EsField> mapping = IndexResolver.mergedMappings(
            "*",
            false,
            new IndexResolver.FieldsInfo(caps, TransportVersion.current(), false, false, false, false, false, true),
            false,
            IndexResolver.DO_NOT_GROUP
        ).get().mapping();
        return EsqlSession.hasFlattenedField(mapping);
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
