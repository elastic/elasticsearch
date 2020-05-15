/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.annotations;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;

import static org.elasticsearch.common.collect.Tuple.tuple;
import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AnnotationPersisterTests extends ESTestCase {

    private static final String ANNOTATION_ID = "existing_annotation_id";
    private static final String ERROR_MESSAGE = "an error occurred while persisting annotation";

    private Client client;
    private AbstractAuditor auditor;
    private IndexResponse indexResponse;

    private ArgumentCaptor<IndexRequest> indexRequestCaptor;

    @Before
    public void setUpMocks() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        auditor = mock(AbstractAuditor.class);

        indexResponse = mock(IndexResponse.class);
        when(indexResponse.getId()).thenReturn(ANNOTATION_ID);

        indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
    }

    public void testPersistAnnotation_Create() throws IOException {
        doReturn(instantFuture(indexResponse)).when(client).index(any());

        AnnotationPersister persister = new AnnotationPersister(client, auditor);
        Annotation annotation = AnnotationTests.randomAnnotation();
        Tuple<String, Annotation> result = persister.persistAnnotation(null, annotation, ERROR_MESSAGE);
        assertThat(result, is(equalTo(tuple(ANNOTATION_ID, annotation))));

        InOrder inOrder = inOrder(client);
        inOrder.verify(client).threadPool();
        inOrder.verify(client).index(indexRequestCaptor.capture());
        verifyNoMoreInteractions(client, auditor);

        IndexRequest indexRequest = indexRequestCaptor.getValue();

        assertThat(indexRequest.index(), is(equalTo(AnnotationIndex.WRITE_ALIAS_NAME)));
        assertThat(indexRequest.id(), is(nullValue()));
        assertThat(parseAnnotation(indexRequest.source()), is(equalTo(annotation)));
        assertThat(indexRequest.opType(), equalTo(DocWriteRequest.OpType.INDEX));
    }

    public void testPersistAnnotation_Update() throws IOException {
        doReturn(instantFuture(indexResponse)).when(client).index(any());

        AnnotationPersister persister = new AnnotationPersister(client, auditor);
        Annotation annotation = AnnotationTests.randomAnnotation();
        Tuple<String, Annotation> result = persister.persistAnnotation(ANNOTATION_ID, annotation, ERROR_MESSAGE);
        assertThat(result, is(equalTo(tuple(ANNOTATION_ID, annotation))));

        InOrder inOrder = inOrder(client);
        inOrder.verify(client).threadPool();
        inOrder.verify(client).index(indexRequestCaptor.capture());
        verifyNoMoreInteractions(client, auditor);

        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.index(), is(equalTo(AnnotationIndex.WRITE_ALIAS_NAME)));
        assertThat(indexRequest.id(), is(equalTo(ANNOTATION_ID)));
        assertThat(parseAnnotation(indexRequest.source()), is(equalTo(annotation)));
        assertThat(indexRequest.opType(), equalTo(DocWriteRequest.OpType.INDEX));
    }

    @SuppressWarnings("unchecked")
    private static <T> ActionFuture<T> instantFuture(T response) {
        ActionFuture future = mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(response);
        return future;
    }

    private Annotation parseAnnotation(BytesReference source) throws IOException {
        try (XContentParser parser = createParser(jsonXContent, source)) {
            return Annotation.PARSER.parse(parser, null).build();
        }
    }
}
