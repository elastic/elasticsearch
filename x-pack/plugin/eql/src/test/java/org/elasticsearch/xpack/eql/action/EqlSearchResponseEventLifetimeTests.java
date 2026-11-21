/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@link EqlSearchResponse.Event#Event(SearchHit)} retains {@link SearchHit#getSourceRef()} and copies document fields so the
 * event outlives {@link SearchHit#decRef()}.
 */
public class EqlSearchResponseEventLifetimeTests extends ESTestCase {

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

    public void testSerializingEventAfterReleasingSearchHitKeepsFetchFields() throws IOException {
        SearchHit hit = new SearchHit(0, "id");
        hit.setDocumentField(new DocumentField("user", List.of("u1")));
        hit.setDocumentField(new DocumentField("action", List.of("a")));

        EqlSearchResponse.Event event = new EqlSearchResponse.Event(hit);
        hit.decRef();

        assertThat(event.fetchFields().keySet(), containsInAnyOrder("user", "action"));
        assertThat(event.fetchFields().get("user").getValues(), equalTo(List.of("u1")));
        assertThat(event.fetchFields().get("action").getValues(), equalTo(List.of("a")));

        Map<String, Object> rendered;
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            event.toXContent(builder, ToXContent.EMPTY_PARAMS);
            rendered = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
        assertThat(rendered.get("fields"), equalTo(Map.of("user", List.of("u1"), "action", List.of("a"))));
    }
}
