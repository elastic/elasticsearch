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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Comparator;

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

        verifyDocs("metrics-bar", numDocsBar);
        verifyDocs("metrics-foo", numDocsFoo);

        // TODO: execute rollover and index some more data.

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request("metrics-*");
        client().admin().indices().deleteDataStream(deleteDataStreamRequest).actionGet();
        getDataStreamResponse = client().admin().indices().getDataStreams(getDataStreamRequest).actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(0));

        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices("metrics-bar-000001")).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices("metrics-foo-000001")).actionGet());
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

    private static void verifyDocs(String dataStream, long expectedNumHits) {
        SearchRequest searchRequest = new SearchRequest(dataStream);
        searchRequest.source().size((int) expectedNumHits);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(expectedNumHits));
        Arrays.stream(searchResponse.getHits().getHits()).forEach(hit -> {
            assertThat(hit.getIndex(), equalTo(dataStream + "-000001"));
        });
    }

}
