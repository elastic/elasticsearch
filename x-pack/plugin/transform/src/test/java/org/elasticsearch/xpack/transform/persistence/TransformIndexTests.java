/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.transforms.DestAlias;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.atLeastOnce;

public class TransformIndexTests extends ESTestCase {

    private static final String TRANSFORM_ID = "some-random-transform-id";
    private static final int CURRENT_TIME_MILLIS = 123456789;
    private static final String DEST_INDEX = "some-dest-index";
    private static final String CREATED_BY = "transform";

    private Client client;
    private final Clock clock = Clock.fixed(Instant.ofEpochMilli(CURRENT_TIME_MILLIS), ZoneId.systemDefault());

    @Before
    public void setUpMocks() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
    }

    public void testIsDestinationIndexCreatedByTransform_NoIndex() throws Exception {
        doAnswer(withFailure(new IndexNotFoundException(DEST_INDEX))).when(client).execute(eq(GetIndexAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        TransformIndex.isDestinationIndexCreatedByTransform(
            client,
            DEST_INDEX,
            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(Assert::assertFalse), latch)
        );
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private void testIsDestinationIndexCreatedByTransform(Map<String, MappingMetadata> mappings, boolean expectedValue) throws Exception {
        GetIndexResponse getIndexResponse = new GetIndexResponse(new String[] { DEST_INDEX }, mappings, null, null, null, null);
        doAnswer(withResponse(getIndexResponse)).when(client).execute(eq(GetIndexAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        TransformIndex.isDestinationIndexCreatedByTransform(
            client,
            DEST_INDEX,
            new LatchedActionListener<>(
                ActionTestUtils.assertNoFailureListener(value -> assertThat(value, is(equalTo(expectedValue)))),
                latch
            )
        );
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testIsDestinationIndexCreatedByTransform_NoMappings() throws Exception {
        testIsDestinationIndexCreatedByTransform(null, false);
    }

    public void testIsDestinationIndexCreatedByTransform_NoIndexInMappings() throws Exception {
        testIsDestinationIndexCreatedByTransform(Map.of(), false);
    }

    public void testIsDestinationIndexCreatedByTransform_NoMeta() throws Exception {
        testIsDestinationIndexCreatedByTransform(Map.of(DEST_INDEX, MappingMetadata.EMPTY_MAPPINGS), false);
    }

    public void testIsDestinationIndexCreatedByTransform_NoCreatedBy() throws Exception {
        Map<String, MappingMetadata> mappings = Map.of(DEST_INDEX, new MappingMetadata("_doc", Map.of("_meta", Map.of())));
        testIsDestinationIndexCreatedByTransform(mappings, false);
    }

    public void testIsDestinationIndexCreatedByTransform_CreatedByDoesNotMatch() throws Exception {
        Map<String, MappingMetadata> mappings = Map.of(
            DEST_INDEX,
            new MappingMetadata("_doc", Map.of("_meta", Map.of("created_by", "some-user")))
        );
        testIsDestinationIndexCreatedByTransform(mappings, false);
    }

    public void testIsDestinationIndexCreatedByTransform_Ok() throws Exception {
        Map<String, MappingMetadata> mappings = Map.of(
            DEST_INDEX,
            new MappingMetadata("_doc", Map.of("_meta", Map.of("created_by", CREATED_BY)))
        );
        testIsDestinationIndexCreatedByTransform(mappings, true);
    }

    public void testCreateDestinationIndex() throws IOException {
        doAnswer(withResponse(null)).when(client).execute(any(), any(), any());

        TransformIndex.createDestinationIndex(
            client,
            TransformConfigTests.randomTransformConfig(TRANSFORM_ID),
            TransformIndex.createTransformDestIndexSettings(new HashMap<>(), TRANSFORM_ID, clock),
            ActionTestUtils.assertNoFailureListener(Assert::assertTrue)
        );

        ArgumentCaptor<CreateIndexRequest> createIndexRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        verify(client).execute(eq(CreateIndexAction.INSTANCE), createIndexRequestCaptor.capture(), any());
        verify(client, atLeastOnce()).threadPool();
        verifyNoMoreInteractions(client);

        CreateIndexRequest createIndexRequest = createIndexRequestCaptor.getValue();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, createIndexRequest.mappings())) {
            Map<String, Object> map = parser.map();
            assertThat(extractValue("_doc._meta._transform.transform", map), equalTo(TRANSFORM_ID));
            assertThat(extractValue("_doc._meta._transform.creation_date_in_millis", map), equalTo(CURRENT_TIME_MILLIS));
            assertThat(extractValue("_doc._meta.created_by", map), equalTo(CREATED_BY));
        }
        assertThat(createIndexRequest.aliases(), is(empty()));
    }

    public void testSetUpDestinationAliases_NullAliases() {
        doAnswer(withResponse(null)).when(client).execute(any(), any(), any());

        TransformConfig config = new TransformConfig.Builder().setId("my-id")
            .setSource(new SourceConfig("my-source"))
            .setDest(new DestConfig("my-dest", null, null))
            .build();

        TransformIndex.setUpDestinationAliases(client, config, ActionTestUtils.assertNoFailureListener(Assert::assertTrue));

        verifyNoMoreInteractions(client);
    }

    public void testSetUpDestinationAliases_EmptyAliases() {
        doAnswer(withResponse(null)).when(client).execute(any(), any(), any());

        TransformConfig config = new TransformConfig.Builder().setId("my-id")
            .setSource(new SourceConfig("my-source"))
            .setDest(new DestConfig("my-dest", List.of(), null))
            .build();

        TransformIndex.setUpDestinationAliases(client, config, ActionTestUtils.assertNoFailureListener(Assert::assertTrue));

        verifyNoMoreInteractions(client);
    }

    public void testSetUpDestinationAliases() {
        doAnswer(withResponse(null)).when(client).execute(any(), any(), any());

        String destIndex = "my-dest";
        TransformConfig config = new TransformConfig.Builder().setId("my-id")
            .setSource(new SourceConfig("my-source"))
            .setDest(new DestConfig(destIndex, List.of(new DestAlias(".all", false), new DestAlias(".latest", true)), null))
            .build();

        TransformIndex.setUpDestinationAliases(client, config, ActionTestUtils.assertNoFailureListener(Assert::assertTrue));

        ArgumentCaptor<IndicesAliasesRequest> indicesAliasesRequestCaptor = ArgumentCaptor.forClass(IndicesAliasesRequest.class);
        verify(client).execute(eq(IndicesAliasesAction.INSTANCE), indicesAliasesRequestCaptor.capture(), any());
        verify(client, atLeastOnce()).threadPool();
        verifyNoMoreInteractions(client);

        IndicesAliasesRequest indicesAliasesRequest = indicesAliasesRequestCaptor.getValue();
        assertThat(
            indicesAliasesRequest.getAliasActions(),
            contains(
                IndicesAliasesRequest.AliasActions.remove().alias(".latest").index("*"),
                IndicesAliasesRequest.AliasActions.add().alias(".all").index(destIndex),
                IndicesAliasesRequest.AliasActions.add().alias(".latest").index(destIndex)
            )
        );
    }

    public void testCreateMappingsFromStringMap() {
        assertThat(TransformIndex.createMappingsFromStringMap(emptyMap()), is(anEmptyMap()));
        assertThat(
            TransformIndex.createMappingsFromStringMap(singletonMap("a", "long")),
            is(equalTo(singletonMap("a", singletonMap("type", "long"))))
        );
        assertThat(TransformIndex.createMappingsFromStringMap(new HashMap<>() {
            {
                put("a", "long");
                put("b", "keyword");
            }
        }), is(equalTo(new HashMap<>() {
            {
                put("a", singletonMap("type", "long"));
                put("b", singletonMap("type", "keyword"));
            }
        })));
        assertThat(TransformIndex.createMappingsFromStringMap(new HashMap<>() {
            {
                put("a", "long");
                put("a.b", "keyword");
            }
        }), is(equalTo(new HashMap<>() {
            {
                put("a", singletonMap("type", "long"));
                put("a.b", singletonMap("type", "keyword"));
            }
        })));
        assertThat(TransformIndex.createMappingsFromStringMap(new HashMap<>() {
            {
                put("a", "long");
                put("a.b", "text");
                put("a.b.c", "keyword");
            }
        }), is(equalTo(new HashMap<>() {
            {
                put("a", singletonMap("type", "long"));
                put("a.b", singletonMap("type", "text"));
                put("a.b.c", singletonMap("type", "keyword"));
            }
        })));
        assertThat(TransformIndex.createMappingsFromStringMap(new HashMap<>() {
            {
                put("a", "object");
                put("a.b", "long");
                put("c", "nested");
                put("c.d", "boolean");
                put("f", "object");
                put("f.g", "object");
                put("f.g.h", "text");
                put("f.g.h.i", "text");
            }
        }), is(equalTo(new HashMap<>() {
            {
                put("a", singletonMap("type", "object"));
                put("a.b", singletonMap("type", "long"));
                put("c", singletonMap("type", "nested"));
                put("c.d", singletonMap("type", "boolean"));
                put("f", singletonMap("type", "object"));
                put("f.g", singletonMap("type", "object"));
                put("f.g.h", singletonMap("type", "text"));
                put("f.g.h.i", singletonMap("type", "text"));
            }
        })));
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withFailure(Exception e) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onFailure(e);
            return null;
        };
    }
}
