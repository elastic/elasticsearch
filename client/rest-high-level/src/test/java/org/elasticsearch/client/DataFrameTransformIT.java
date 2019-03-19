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

package org.elasticsearch.client;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.dataframe.DeleteDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.PreviewDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.PreviewDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.PutDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.StopDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StopDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.dataframe.transforms.QueryConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.AggregationConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.PivotConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.TermsGroupSource;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class DataFrameTransformIT extends ESRestHighLevelClientTestCase {

    private List<String> transformsToClean = new ArrayList<>();

    private void createIndex(String indexName) throws IOException {

        XContentBuilder builder = jsonBuilder();
        builder.startObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("user_id")
                .field("type", "keyword")
                .endObject()
                .startObject("stars")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject();

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.mapping(builder);
        CreateIndexResponse response = highLevelClient().indices().create(request, RequestOptions.DEFAULT);
        assertTrue(response.isAcknowledged());
    }

    private void indexData(String indexName) throws IOException {
        BulkRequest request = new BulkRequest();
        {
            Map<String, Object> doc = new HashMap<>();
            doc.put("timestamp", "2019-03-10T12:00:00+00");
            doc.put("user_id", "theresa");
            doc.put("stars", 2);
            request.add(new IndexRequest(indexName).source(doc, XContentType.JSON));

            doc = new HashMap<>();
            doc.put("timestamp", "2019-03-10T18:00:00+00");
            doc.put("user_id", "theresa");
            doc.put("stars", 3);
            request.add(new IndexRequest(indexName).source(doc, XContentType.JSON));

            doc = new HashMap<>();
            doc.put("timestamp", "2019-03-10T12:00:00+00");
            doc.put("user_id", "michel");
            doc.put("stars", 5);
            request.add(new IndexRequest(indexName).source(doc, XContentType.JSON));

            doc = new HashMap<>();
            doc.put("timestamp", "2019-03-10T18:00:00+00");
            doc.put("user_id", "michel");
            doc.put("stars", 3);
            request.add(new IndexRequest(indexName).source(doc, XContentType.JSON));

            doc = new HashMap<>();
            doc.put("timestamp", "2019-03-11T12:00:00+00");
            doc.put("user_id", "michel");
            doc.put("stars", 3);
            request.add(new IndexRequest(indexName).source(doc, XContentType.JSON));
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        BulkResponse response = highLevelClient().bulk(request, RequestOptions.DEFAULT);
        assertFalse(response.hasFailures());
    }

    @After
    public void cleanUpTransforms() throws IOException {
        for (String transformId : transformsToClean) {
            highLevelClient().dataFrame().stopDataFrameTransform(new StopDataFrameTransformRequest(transformId), RequestOptions.DEFAULT);
        }

        for (String transformId : transformsToClean) {
            highLevelClient().dataFrame().deleteDataFrameTransform(
                    new DeleteDataFrameTransformRequest(transformId), RequestOptions.DEFAULT);
        }

        transformsToClean = new ArrayList<>();
    }

    public void testCreateDelete() throws IOException {
        String sourceIndex = "transform-source";
        createIndex(sourceIndex);

        QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
        GroupConfig groupConfig = new GroupConfig(Collections.singletonMap("reviewer", new TermsGroupSource("user_id")));
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = new PivotConfig(groupConfig, aggConfig);

        String id = "test-crud";
        DataFrameTransformConfig transform = new DataFrameTransformConfig(id, sourceIndex, "pivot-dest", queryConfig, pivotConfig);

        DataFrameClient client = highLevelClient().dataFrame();
        AcknowledgedResponse ack = execute(new PutDataFrameTransformRequest(transform), client::putDataFrameTransform,
                client::putDataFrameTransformAsync);
        assertTrue(ack.isAcknowledged());

        ack = execute(new DeleteDataFrameTransformRequest(transform.getId()), client::deleteDataFrameTransform,
                client::deleteDataFrameTransformAsync);
        assertTrue(ack.isAcknowledged());

        // The second delete should fail
        ElasticsearchStatusException deleteError = expectThrows(ElasticsearchStatusException.class,
                () -> execute(new DeleteDataFrameTransformRequest(transform.getId()), client::deleteDataFrameTransform,
                        client::deleteDataFrameTransformAsync));
        assertThat(deleteError.getMessage(), containsString("Transform with id [test-crud] could not be found"));
    }

    public void testStartStop() throws IOException {
        String sourceIndex = "transform-source";
        createIndex(sourceIndex);

        QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
        GroupConfig groupConfig = new GroupConfig(Collections.singletonMap("reviewer", new TermsGroupSource("user_id")));
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = new PivotConfig(groupConfig, aggConfig);

        String id = "test-stop-start";
        DataFrameTransformConfig transform = new DataFrameTransformConfig(id, sourceIndex, "pivot-dest", queryConfig, pivotConfig);

        DataFrameClient client = highLevelClient().dataFrame();
        AcknowledgedResponse ack = execute(new PutDataFrameTransformRequest(transform), client::putDataFrameTransform,
                client::putDataFrameTransformAsync);
        assertTrue(ack.isAcknowledged());
        transformsToClean.add(id);

        StartDataFrameTransformRequest startRequest = new StartDataFrameTransformRequest(id);
        StartDataFrameTransformResponse startResponse =
                execute(startRequest, client::startDataFrameTransform, client::startDataFrameTransformAsync);
        assertTrue(startResponse.isStarted());
        assertThat(startResponse.getNodeFailures(), empty());
        assertThat(startResponse.getTaskFailures(), empty());

        // TODO once get df stats is implemented assert the df has started

        StopDataFrameTransformRequest stopRequest = new StopDataFrameTransformRequest(id);
        StopDataFrameTransformResponse stopResponse =
                execute(stopRequest, client::stopDataFrameTransform, client::stopDataFrameTransformAsync);
        assertTrue(stopResponse.isStopped());
        assertThat(stopResponse.getNodeFailures(), empty());
        assertThat(stopResponse.getTaskFailures(), empty());
    }

    public void testPreview() throws IOException {
        String sourceIndex = "transform-source";
        createIndex(sourceIndex);
        indexData(sourceIndex);

        QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
        GroupConfig groupConfig = new GroupConfig(Collections.singletonMap("reviewer", new TermsGroupSource("user_id")));
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = new PivotConfig(groupConfig, aggConfig);

        DataFrameTransformConfig transform = new DataFrameTransformConfig("test-preview", sourceIndex, null, queryConfig, pivotConfig);

        DataFrameClient client = highLevelClient().dataFrame();
        PreviewDataFrameTransformResponse preview = execute(new PreviewDataFrameTransformRequest(transform),
                client::previewDataFrameTransform,
                client::previewDataFrameTransformAsync);

        List<Map<String, Object>> docs = preview.getDocs();
        assertThat(docs, hasSize(2));
        Optional<Map<String, Object>> theresa = docs.stream().filter(doc -> "theresa".equals(doc.get("reviewer"))).findFirst();
        assertTrue(theresa.isPresent());
        assertEquals(2.5d, (double)theresa.get().get("avg_rating"), 0.01d);

        Optional<Map<String, Object>> michel = docs.stream().filter(doc -> "michel".equals(doc.get("reviewer"))).findFirst();
        assertTrue(michel.isPresent());
        assertEquals(3.6d, (double)michel.get().get("avg_rating"), 0.1d);
    }
}

