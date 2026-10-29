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
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.IndexResolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class IndexResolverPassthroughTests extends ESTestCase {

    public void testPassthroughOnlyWhenAllIndicesAgree() {
        assertThat(resolveAttributes(true, true).isPassthrough(), is(true));
        assertThat(resolveAttributes(true, false).isPassthrough(), is(false));
        assertThat(resolveAttributes(true, null).isPassthrough(), is(false));
        assertThat(resolveAttributes(false, false).isPassthrough(), is(false));
    }

    private static EsField resolveAttributes(Boolean first, Boolean second) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder()
            .withIndexResponses(List.of(index("idx-a", first), index("idx-b", second)))
            .build();
        IndexResolution resolution = IndexResolver.mergedMappings(
            "idx-*",
            false,
            new IndexResolver.FieldsInfo(caps, new ArrayList<>(), TransportVersion.current(), false, false, false, false, false, false),
            false,
            IndexResolver.DO_NOT_GROUP
        );
        return resolution.get().mapping().get("attributes");
    }

    private static FieldCapabilitiesIndexResponse index(String index, Boolean isPassthrough) {
        var attributes = new IndexFieldCapabilitiesBuilder("attributes", "object").isPassthrough(isPassthrough).build();
        return new FieldCapabilitiesIndexResponse(index, index, Map.of("attributes", attributes), true, IndexMode.STANDARD, 1, 1L, 1L);
    }
}
