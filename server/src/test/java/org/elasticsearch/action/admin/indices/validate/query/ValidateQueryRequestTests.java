/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ValidateQueryRequestTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(IndicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    public void testSerialization() throws Exception {
        ValidateQueryRequest request = new ValidateQueryRequest(randomBoolean() ? new String[] { "index-a", "index-b" } : new String[0]);
        request.query(QueryBuilders.termQuery("field", randomAlphaOfLength(5)));
        request.explain(randomBoolean());
        request.rewrite(randomBoolean());
        request.allShards(randomBoolean());
        if (randomBoolean()) {
            request.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean() && SliceIndexing.SLICE_FEATURE_FLAG.isEnabled()) {
            String searchSlice = randomBoolean() ? SliceIndexing.SLICE_ALL : randomAlphaOfLengthBetween(3, 10);
            request.searchSlice(searchSlice);
        }

        TransportVersion version = TransportVersionUtils.randomCompatibleVersion();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(version);
            request.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                in.setTransportVersion(version);
                ValidateQueryRequest readRequest = new ValidateQueryRequest(in);
                assertEquals(request.query(), readRequest.query());
                assertEquals(request.explain(), readRequest.explain());
                assertEquals(request.rewrite(), readRequest.rewrite());
                assertEquals(request.allShards(), readRequest.allShards());
                if (version.supports(SliceIndexing.VALIDATE_QUERY_SLICE_ROUTING_STATE_VERSION)) {
                    assertEquals(request.routing(), readRequest.routing());
                    assertEquals(request.searchSlice(), readRequest.searchSlice());
                    assertEquals(request.isRoutingFromSlice(), readRequest.isRoutingFromSlice());
                } else {
                    assertNull(readRequest.routing());
                    assertNull(readRequest.searchSlice());
                    assertFalse(readRequest.isRoutingFromSlice());
                }
            }
        }
    }

    public void testSearchSliceDerivesRoutingAndProvenance() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        ValidateQueryRequest request = new ValidateQueryRequest();
        request.routing("manual");
        request.searchSlice("s1,s2");
        assertEquals("s1,s2", request.searchSlice());
        assertEquals("s1,s2", request.routing());
        assertTrue(request.isRoutingFromSlice());

        request.searchSlice(SliceIndexing.SLICE_ALL);
        assertEquals(SliceIndexing.SLICE_ALL, request.searchSlice());
        assertNull(request.routing());
        assertTrue(request.isRoutingFromSlice());
    }

    public void testClearingSearchSliceClearsDerivedRouting() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        ValidateQueryRequest request = new ValidateQueryRequest().searchSlice("s1");
        request.searchSlice(null);
        assertNull(request.searchSlice());
        assertFalse(request.isRoutingFromSlice());

        assertNull(request.routing());
        request.routing("manual");
        assertEquals("manual", request.routing());
    }
}
