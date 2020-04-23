/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.elasticsearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.elasticsearch.action.admin.indices.datastream.GetDataStreamsAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DataStreamIT extends ESIntegTestCase {

    public void testBasicScenario() throws Exception {
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("metrics-foo");
        createDataStreamRequest.setTimestampFieldName("@timestamp1");
        client().admin().indices().createDataStream(createDataStreamRequest).get();

        createDataStreamRequest = new CreateDataStreamAction.Request("metrics-bar");
        createDataStreamRequest.setTimestampFieldName("@timestamp2");
        client().admin().indices().createDataStream(createDataStreamRequest).get();

        GetDataStreamsAction.Request getDataStreamRequest = new GetDataStreamsAction.Request("*");
        GetDataStreamsAction.Response getDataStreamResponse = client().admin().indices().getDataStreams(getDataStreamRequest).actionGet();
        getDataStreamResponse.getDataStreams().sort(Comparator.comparing(DataStream::getName));
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(2));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getName(), equalTo("metrics-bar"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getTimeStampField(), equalTo("@timestamp2"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getIndices().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getIndices().get(0).getName(), equalTo("metrics-bar-000001"));
        assertThat(getDataStreamResponse.getDataStreams().get(1).getName(), equalTo("metrics-foo"));
        assertThat(getDataStreamResponse.getDataStreams().get(1).getTimeStampField(), equalTo("@timestamp1"));
        assertThat(getDataStreamResponse.getDataStreams().get(1).getIndices().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(1).getIndices().get(0).getName(), equalTo("metrics-foo-000001"));

        GetIndexResponse getIndexResponse =
            client().admin().indices().getIndex(new GetIndexRequest().indices("metrics-bar-000001")).actionGet();
        assertThat(getIndexResponse.getSettings().get("metrics-bar-000001"), notNullValue());
        assertThat(getIndexResponse.getSettings().get("metrics-bar-000001").getAsBoolean("index.hidden", null), is(true));

        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices("metrics-foo-000001")).actionGet();
        assertThat(getIndexResponse.getSettings().get("metrics-foo-000001"), notNullValue());
        assertThat(getIndexResponse.getSettings().get("metrics-foo-000001").getAsBoolean("index.hidden", null), is(true));

        int numDocsBar = randomIntBetween(2, 16);
        indexDocs("metrics-bar", numDocsBar);
        int numDocsFoo = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsFoo);

        verifyDocs("metrics-bar", numDocsBar, 1, 1);
        verifyDocs("metrics-foo", numDocsFoo, 1, 1);

        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("metrics-foo", null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo("metrics-foo-000002"));
        assertTrue(rolloverResponse.isRolledOver());

        rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("metrics-bar", null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo("metrics-bar-000002"));
        assertTrue(rolloverResponse.isRolledOver());

        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices("metrics-foo-000002")).actionGet();
        assertThat(getIndexResponse.getSettings().get("metrics-foo-000002"), notNullValue());
        assertThat(getIndexResponse.getSettings().get("metrics-foo-000002").getAsBoolean("index.hidden", null), is(true));

        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices("metrics-bar-000002")).actionGet();
        assertThat(getIndexResponse.getSettings().get("metrics-bar-000002"), notNullValue());
        assertThat(getIndexResponse.getSettings().get("metrics-bar-000002").getAsBoolean("index.hidden", null), is(true));

        int numDocsBar2 = randomIntBetween(2, 16);
        indexDocs("metrics-bar", numDocsBar2);
        int numDocsFoo2 = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsFoo2);

        verifyDocs("metrics-bar", numDocsBar + numDocsBar2, 1, 2);
        verifyDocs("metrics-foo", numDocsFoo + numDocsFoo2, 1, 2);

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request("metrics-*");
        client().admin().indices().deleteDataStream(deleteDataStreamRequest).actionGet();
        getDataStreamResponse = client().admin().indices().getDataStreams(getDataStreamRequest).actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(0));

        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices("metrics-bar-000001")).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices("metrics-bar-000002")).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices("metrics-foo-000001")).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices("metrics-foo-000002")).actionGet());
    }

    public void testOtherWriteOps() throws Exception {
        String dataStreamName = "metrics-foobar";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        createDataStreamRequest.setTimestampFieldName("@timestamp1");
        client().admin().indices().createDataStream(createDataStreamRequest).get();

        {
            BulkRequest bulkRequest = new BulkRequest()
                .add(new IndexRequest(dataStreamName).source("{}", XContentType.JSON));
            expectFailure(dataStreamName, () -> client().bulk(bulkRequest).actionGet());
        }
        {
            BulkRequest bulkRequest = new BulkRequest()
                .add(new DeleteRequest(dataStreamName, "_id"));
            expectFailure(dataStreamName, () -> client().bulk(bulkRequest).actionGet());
        }
        {
            BulkRequest bulkRequest = new BulkRequest()
                .add(new UpdateRequest(dataStreamName, "_id").doc("{}", XContentType.JSON));
            expectFailure(dataStreamName, () -> client().bulk(bulkRequest).actionGet());
        }
        {
            IndexRequest indexRequest = new IndexRequest(dataStreamName).source("{}", XContentType.JSON);
            expectFailure(dataStreamName, () -> client().index(indexRequest).actionGet());
        }
        {
            UpdateRequest updateRequest = new UpdateRequest(dataStreamName, "_id")
                .doc("{}", XContentType.JSON);
            expectFailure(dataStreamName, () -> client().update(updateRequest).actionGet());
        }
        {
            DeleteRequest deleteRequest = new DeleteRequest(dataStreamName, "_id");
            expectFailure(dataStreamName, () -> client().delete(deleteRequest).actionGet());
        }
        {
            IndexRequest indexRequest = new IndexRequest(dataStreamName).source("{}", XContentType.JSON)
                .opType(DocWriteRequest.OpType.CREATE);
            IndexResponse indexResponse = client().index(indexRequest).actionGet();
            assertThat(indexResponse.getIndex(), equalTo(DataStream.getBackingIndexName(dataStreamName, 1)));
        }
        {
            BulkRequest bulkRequest = new BulkRequest()
                .add(new IndexRequest(dataStreamName).source("{}", XContentType.JSON)
                    .opType(DocWriteRequest.OpType.CREATE));
            BulkResponse bulkItemResponses  = client().bulk(bulkRequest).actionGet();
            assertThat(bulkItemResponses.getItems()[0].getIndex(), equalTo(DataStream.getBackingIndexName(dataStreamName, 1)));
        }

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request("*");
        client().admin().indices().deleteDataStream(deleteDataStreamRequest).actionGet();
    }

    private static void indexDocs(String dataStream, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(dataStream)
                .opType(DocWriteRequest.OpType.CREATE)
                .source("{}", XContentType.JSON));
        }
        client().bulk(bulkRequest).actionGet();
        client().admin().indices().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    private static void verifyDocs(String dataStream, long expectedNumHits, long minGeneration, long maxGeneration) {
        SearchRequest searchRequest = new SearchRequest(dataStream);
        searchRequest.source().size((int) expectedNumHits);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(expectedNumHits));

        List<String> expectedIndices = new ArrayList<>();
        for (long k = minGeneration; k <= maxGeneration; k++) {
            expectedIndices.add(DataStream.getBackingIndexName(dataStream, k));
        }
        Arrays.stream(searchResponse.getHits().getHits()).forEach(hit -> {
            assertTrue(expectedIndices.contains(hit.getIndex()));
        });
    }

    private static void expectFailure(String dataStreamName, ThrowingRunnable runnable) {
        Exception e = expectThrows(IllegalArgumentException.class, runnable);
        assertThat(e.getMessage(), equalTo("The provided expression [" + dataStreamName +
            "] matches a data stream, specify the corresponding concrete indices instead."));
    }

}
