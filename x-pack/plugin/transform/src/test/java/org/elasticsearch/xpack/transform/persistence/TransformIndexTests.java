/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransformIndexTests extends ESTestCase {

    private static final String TRANSFORM_ID = "some-random-transform-id";
    private static final int CURRENT_TIME_MILLIS = 123456789;
    private static final String CREATED_BY = "transform";

    private Client client = mock(Client.class);
    private Clock clock = Clock.fixed(Instant.ofEpochMilli(CURRENT_TIME_MILLIS), ZoneId.systemDefault());

    public void testCreateDestinationIndex() throws IOException {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        TransformIndex.createDestinationIndex(
            client,
            TransformConfigTests.randomTransformConfig(TRANSFORM_ID),
            TransformIndex.createTransformDestIndexSettings(new HashMap<>(), TRANSFORM_ID, clock),
            ActionListener.wrap(value -> assertTrue(value), e -> fail(e.getMessage()))
        );

        ArgumentCaptor<CreateIndexRequest> createIndexRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        verify(client).execute(eq(CreateIndexAction.INSTANCE), createIndexRequestCaptor.capture(), any());
        verifyNoMoreInteractions(client);

        CreateIndexRequest createIndexRequest = createIndexRequestCaptor.getValue();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, createIndexRequest.mappings())) {
            Map<String, Object> map = parser.map();
            assertThat(extractValue("_doc._meta._transform.transform", map), equalTo(TRANSFORM_ID));
            assertThat(extractValue("_doc._meta._transform.creation_date_in_millis", map), equalTo(CURRENT_TIME_MILLIS));
            assertThat(extractValue("_doc._meta.created_by", map), equalTo(CREATED_BY));
        }
    }

    public void testCreateMappingsFromStringMap() {
        assertThat(TransformIndex.createMappingsFromStringMap(emptyMap()), is(anEmptyMap()));
        assertThat(
            TransformIndex.createMappingsFromStringMap(singletonMap("a", "long")),
            is(equalTo(singletonMap("a", singletonMap("type", "long"))))
        );
        assertThat(
            TransformIndex.createMappingsFromStringMap(new HashMap<>() {{
                put("a", "long");
                put("b", "keyword");
            }}),
            is(equalTo(new HashMap<>() {{
                put("a", singletonMap("type", "long"));
                put("b", singletonMap("type", "keyword"));
            }}))
        );
        assertThat(
            TransformIndex.createMappingsFromStringMap(new HashMap<>() {{
                put("a", "long");
                put("a.b", "keyword");
            }}),
            is(equalTo(new HashMap<>() {{
                put("a", singletonMap("type", "long"));
                put("a.b", singletonMap("type", "keyword"));
            }}))
        );
        assertThat(
            TransformIndex.createMappingsFromStringMap(new HashMap<>() {{
                put("a", "long");
                put("a.b", "text");
                put("a.b.c", "keyword");
            }}),
            is(equalTo(new HashMap<>() {{
                put("a", singletonMap("type", "long"));
                put("a.b", singletonMap("type", "text"));
                put("a.b.c", singletonMap("type", "keyword"));
            }}))
        );
        assertThat(
            TransformIndex.createMappingsFromStringMap(new HashMap<>() {{
                put("a", "object");
                put("a.b", "long");
                put("c", "nested");
                put("c.d", "boolean");
                put("f", "object");
                put("f.g", "object");
                put("f.g.h", "text");
                put("f.g.h.i", "text");
            }}),
            is(equalTo(new HashMap<>() {{
                put("a", singletonMap("type", "object"));
                put("a.b", singletonMap("type", "long"));
                put("c", singletonMap("type", "nested"));
                put("c.d", singletonMap("type", "boolean"));
                put("f", singletonMap("type", "object"));
                put("f.g", singletonMap("type", "object"));
                put("f.g.h", singletonMap("type", "text"));
                put("f.g.h.i", singletonMap("type", "text"));
            }}))
        );
    }
}
