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
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.dataframe.DeleteDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.PutDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.dataframe.transforms.QueryConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.AggregationConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.PivotConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.TermsGroupSource;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class DataFrameTransformIT extends ESRestHighLevelClientTestCase {

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
}

