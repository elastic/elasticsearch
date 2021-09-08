/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.annotations;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationTests;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterServiceTests;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AnnotationPersisterTests extends ESTestCase {

    private static final String ANNOTATION_ID = "existing_annotation_id";
    private static final String JOB_ID = "job_id";

    private Client client;
    private ResultsPersisterService resultsPersisterService;

    private ArgumentCaptor<BulkRequest> bulkRequestCaptor;

    @Before
    public void setUpMocks() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        OriginSettingClient originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);
        resultsPersisterService = ResultsPersisterServiceTests.buildResultsPersisterService(originSettingClient);

        bulkRequestCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    }

    @After
    public void verifyNoMoreInteractionsWithMocks() {
        verify(client, atLeastOnce()).settings();
        verify(client, atLeastOnce()).threadPool();
    }

    public void testPersistAnnotation_Create() throws IOException {
        doAnswer(withResponse(new BulkResponse(new BulkItemResponse[]{ bulkItemSuccess(ANNOTATION_ID) }, 0L)))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        AnnotationPersister persister = new AnnotationPersister(resultsPersisterService);
        Annotation annotation = AnnotationTests.randomAnnotation(JOB_ID);
        Tuple<String, Annotation> result = persister.persistAnnotation(null, annotation);
        assertThat(result, is(equalTo(tuple(ANNOTATION_ID, annotation))));

        verify(client).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());

        List<BulkRequest> bulkRequests = bulkRequestCaptor.getAllValues();
        assertThat(bulkRequests, hasSize(1));
        BulkRequest bulkRequest = bulkRequests.get(0);
        assertThat(bulkRequest.numberOfActions(), equalTo(1));

        IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);
        assertThat(indexRequest.index(), is(equalTo(AnnotationIndex.WRITE_ALIAS_NAME)));
        assertThat(indexRequest.id(), is(nullValue()));
        assertThat(parseAnnotation(indexRequest.source()), is(equalTo(annotation)));
        assertThat(indexRequest.opType(), equalTo(DocWriteRequest.OpType.INDEX));
    }

    public void testPersistAnnotation_Update() throws IOException {
        doAnswer(withResponse(new BulkResponse(new BulkItemResponse[]{ bulkItemSuccess(ANNOTATION_ID) }, 0L)))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        AnnotationPersister persister = new AnnotationPersister(resultsPersisterService);
        Annotation annotation = AnnotationTests.randomAnnotation(JOB_ID);
        Tuple<String, Annotation> result = persister.persistAnnotation(ANNOTATION_ID, annotation);
        assertThat(result, is(equalTo(tuple(ANNOTATION_ID, annotation))));

        verify(client).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());

        List<BulkRequest> bulkRequests = bulkRequestCaptor.getAllValues();
        assertThat(bulkRequests, hasSize(1));
        BulkRequest bulkRequest = bulkRequests.get(0);
        assertThat(bulkRequest.numberOfActions(), equalTo(1));

        IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);
        assertThat(indexRequest.index(), is(equalTo(AnnotationIndex.WRITE_ALIAS_NAME)));
        assertThat(indexRequest.id(), is(equalTo(ANNOTATION_ID)));
        assertThat(parseAnnotation(indexRequest.source()), is(equalTo(annotation)));
        assertThat(indexRequest.opType(), equalTo(DocWriteRequest.OpType.INDEX));
    }

    public void testPersistMultipleAnnotationsWithBulk() {
        doAnswer(withResponse(new BulkResponse(new BulkItemResponse[]{ bulkItemSuccess(ANNOTATION_ID) }, 0L)))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        AnnotationPersister persister = new AnnotationPersister(resultsPersisterService);
        persister.bulkPersisterBuilder(JOB_ID)
            .persistAnnotation(AnnotationTests.randomAnnotation(JOB_ID))
            .persistAnnotation(AnnotationTests.randomAnnotation(JOB_ID))
            .persistAnnotation(AnnotationTests.randomAnnotation(JOB_ID))
            .persistAnnotation(AnnotationTests.randomAnnotation(JOB_ID))
            .persistAnnotation(AnnotationTests.randomAnnotation(JOB_ID))
            .executeRequest();

        verify(client).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());

        List<BulkRequest> bulkRequests = bulkRequestCaptor.getAllValues();
        assertThat(bulkRequests, hasSize(1));
        assertThat(bulkRequests.get(0).numberOfActions(), equalTo(5));
    }

    public void testPersistMultipleAnnotationsWithBulk_LowBulkLimit() {
        doAnswer(withResponse(new BulkResponse(new BulkItemResponse[]{ bulkItemSuccess(ANNOTATION_ID) }, 0L)))
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        AnnotationPersister persister = new AnnotationPersister(resultsPersisterService, 2);
        persister.bulkPersisterBuilder(JOB_ID)
            .persistAnnotation(AnnotationTests.randomAnnotation(JOB_ID))
            .persistAnnotation(AnnotationTests.randomAnnotation(JOB_ID))
            .persistAnnotation(AnnotationTests.randomAnnotation(JOB_ID))
            .persistAnnotation(AnnotationTests.randomAnnotation(JOB_ID))
            .persistAnnotation(AnnotationTests.randomAnnotation(JOB_ID))
            .executeRequest();

        verify(client, times(3)).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());

        List<BulkRequest> bulkRequests = bulkRequestCaptor.getAllValues();
        assertThat(bulkRequests, hasSize(3));
        assertThat(bulkRequests.get(0).numberOfActions(), equalTo(2));
        assertThat(bulkRequests.get(1).numberOfActions(), equalTo(2));
        assertThat(bulkRequests.get(2).numberOfActions(), equalTo(1));
    }

    public void testPersistMultipleAnnotationsWithBulk_EmptyRequest() {
        AnnotationPersister persister = new AnnotationPersister(resultsPersisterService);
        assertThat(persister.bulkPersisterBuilder(JOB_ID).executeRequest(), is(nullValue()));
    }

    public void testPersistMultipleAnnotationsWithBulk_Failure() {
        doAnswer(withResponse(new BulkResponse(new BulkItemResponse[]{bulkItemFailure("1"), bulkItemFailure("2")}, 0L)))  // (1)
            .doAnswer(withResponse(new BulkResponse(new BulkItemResponse[]{bulkItemSuccess("1"), bulkItemFailure("2")}, 0L)))  // (2)
            .doAnswer(withResponse(new BulkResponse(new BulkItemResponse[]{bulkItemFailure("2")}, 0L)))  // (3)
            .when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        AnnotationPersister persister = new AnnotationPersister(resultsPersisterService);
        AnnotationPersister.Builder persisterBuilder = persister.bulkPersisterBuilder(JOB_ID)
            .persistAnnotation("1", AnnotationTests.randomAnnotation(JOB_ID))
            .persistAnnotation("2", AnnotationTests.randomAnnotation(JOB_ID));
        ElasticsearchException e = expectThrows(ElasticsearchException.class, persisterBuilder::executeRequest);
        assertThat(e.getMessage(), containsString("Failed execution"));

        verify(client, atLeastOnce()).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());

        List<BulkRequest> bulkRequests = bulkRequestCaptor.getAllValues();
        assertThat(bulkRequests.get(0).numberOfActions(), equalTo(2));  // Original bulk request of size 2
        assertThat(bulkRequests.get(1).numberOfActions(), equalTo(2));  // Bulk request created from two failures returned in (1)
        for (int i = 2; i < bulkRequests.size(); ++i) {
            assertThat(bulkRequests.get(i).numberOfActions(), equalTo(1));  // Bulk request created from one failure returned in (2) and (3)
        }
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }

    private static BulkItemResponse bulkItemSuccess(String docId) {
        return BulkItemResponse.success(
            1,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId(AnnotationIndex.WRITE_ALIAS_NAME, "uuid", 1), docId, 0, 0, 1, true));
    }

    private static BulkItemResponse bulkItemFailure(String docId) {
        return BulkItemResponse.failure(
            2,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("my-index", docId, new Exception("boom")));
    }

    private Annotation parseAnnotation(BytesReference source) throws IOException {
        try (XContentParser parser = createParser(jsonXContent, source)) {
            return Annotation.fromXContent(parser, null);
        }
    }
}
