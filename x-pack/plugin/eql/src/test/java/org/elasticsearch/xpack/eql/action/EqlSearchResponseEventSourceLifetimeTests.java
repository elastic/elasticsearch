/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * {@link EqlSearchResponse.Event#Event(SearchHit)} retains {@link SearchHit#getSourceRef()} so the event outlives
 * {@link SearchHit#decRef()}.
 */
public class EqlSearchResponseEventSourceLifetimeTests extends ESTestCase {

    public void testSerializingEventAfterReleasingSearchHitSucceedsWithOwnedSource() throws IOException {
        BytesArray inner = new BytesArray("{\"k\":\"v\"}".getBytes(StandardCharsets.UTF_8));
        ReleasableBytesReference source = new ReleasableBytesReference(inner, () -> {});

        SearchHit hit = new SearchHit(0, "id");
        hit.sourceRef(source);

        EqlSearchResponse.Event event = new EqlSearchResponse.Event(hit);
        hit.decRef();

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            event.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }

        EqlSearchResponse response = new EqlSearchResponse(
            new EqlSearchResponse.Hits(Collections.singletonList(event), null, null),
            0,
            false,
            ShardSearchFailure.EMPTY_ARRAY
        );
        assertTrue(response.hasReferences());
        assertTrue(response.decRef());
        assertFalse(response.hasReferences());
    }
}
