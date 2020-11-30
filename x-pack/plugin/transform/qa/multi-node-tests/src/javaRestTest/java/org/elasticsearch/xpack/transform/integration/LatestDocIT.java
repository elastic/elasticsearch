/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.transform.PreviewTransformRequest;
import org.elasticsearch.client.transform.PreviewTransformResponse;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.latest.LatestDocConfig;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class LatestDocIT extends TransformIntegTestCase {

    private static final String SOURCE_INDEX_NAME = "basic-crud-latest-reviews";
    private static final int NUM_USERS = 28;

    private static final String TRANSFORM_NAME = "transform-crud-latest";

    private static final String getDateStringForRow(int row) {
        int month = 1 + (row / 28);
        int day = 1 + (row % 28);
        return "2017-" + (month < 10 ? "0" + month : month) + "-" + (day < 10 ? "0" + day : day) + "T12:30:00Z";
    }

    private static final String TIMESTAMP = "timestamp";
    private static final String USER_ID = "user_id";
    private static final String BUSINESS_ID = "business_id";
    private static final String COUNT = "count";
    private static final String STARS = "stars";

    private static final Map<String, Object> row(String timestamp, String userId, String businessId, int count, int stars) {
        return new HashMap<>() {{
            put(TIMESTAMP, timestamp);
            put(USER_ID, userId);
            put(BUSINESS_ID, businessId);
            put(COUNT, count);
            put(STARS, stars);
        }};
    }

    private static final Object[] EXPECTED_DEST_INDEX_ROWS =
        new Object[] {
            row("2017-04-01T12:30:00Z", "user_0", "business_34", 84, 4),
            row("2017-04-02T12:30:00Z", "user_1", "business_35", 85, 0),
            row("2017-04-03T12:30:00Z", "user_2", "business_36", 86, 1),
            row("2017-04-04T12:30:00Z", "user_3", "business_37", 87, 2),
            row("2017-04-05T12:30:00Z", "user_4", "business_38", 88, 3),
            row("2017-04-06T12:30:00Z", "user_5", "business_39", 89, 4),
            row("2017-04-07T12:30:00Z", "user_6", "business_40", 90, 0),
            row("2017-04-08T12:30:00Z", "user_7", "business_41", 91, 1),
            row("2017-04-09T12:30:00Z", "user_8", "business_42", 92, 2),
            row("2017-04-10T12:30:00Z", "user_9", "business_43", 93, 3),
            row("2017-04-11T12:30:00Z", "user_10", "business_44", 94, 4),
            row("2017-04-12T12:30:00Z", "user_11", "business_45", 95, 0),
            row("2017-04-13T12:30:00Z", "user_12", "business_46", 96, 1),
            row("2017-04-14T12:30:00Z", "user_13", "business_47", 97, 2),
            row("2017-04-15T12:30:00Z", "user_14", "business_48", 98, 3),
            row("2017-04-16T12:30:00Z", "user_15", "business_49", 99, 4),
            row("2017-03-17T12:30:00Z", "user_16", "business_22", 72, 2),
            row("2017-03-18T12:30:00Z", "user_17", "business_23", 73, 3),
            row("2017-03-19T12:30:00Z", "user_18", "business_24", 74, 4),
            row("2017-03-20T12:30:00Z", "user_19", "business_25", 75, 0),
            row("2017-03-21T12:30:00Z", "user_20", "business_26", 76, 1),
            row("2017-03-22T12:30:00Z", "user_21", "business_27", 77, 2),
            row("2017-03-23T12:30:00Z", "user_22", "business_28", 78, 3),
            row("2017-03-24T12:30:00Z", "user_23", "business_29", 79, 4),
            row("2017-03-25T12:30:00Z", "user_24", "business_30", 80, 0),
            row("2017-03-26T12:30:00Z", "user_25", "business_31", 81, 1),
            row("2017-03-27T12:30:00Z", "user_26", "business_32", 82, 2),
            row("2017-03-28T12:30:00Z", "user_27", "business_33", 83, 3)
        };

    @After
    public void cleanTransforms() throws IOException {
        cleanUp();
    }

    public void testLatestDoc() throws Exception {
        createReviewsIndex(SOURCE_INDEX_NAME, 100, NUM_USERS, LatestDocIT::getDateStringForRow);

        String destIndexName = "reviews-latest";
        TransformConfig transformConfig =
            createTransformConfigBuilder(TRANSFORM_NAME, destIndexName, SOURCE_INDEX_NAME)
                .setLatestDocConfig(
                    LatestDocConfig.builder()
                        .setUniqueKey(USER_ID)
                        .setSort(SortBuilders.fieldSort(TIMESTAMP).order(SortOrder.DESC))
                        .build())
                .build();
        assertTrue(putTransform(transformConfig, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startTransform(transformConfig.getId(), RequestOptions.DEFAULT).isAcknowledged());
        waitUntilCheckpoint(transformConfig.getId(), 1L);
        stopTransform(transformConfig.getId());

        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            restClient.indices().refresh(new RefreshRequest(destIndexName), RequestOptions.DEFAULT);
            // Verify destination index mappings
            GetMappingsResponse sourceIndexMapping =
                restClient.indices().getMapping(new GetMappingsRequest().indices(SOURCE_INDEX_NAME), RequestOptions.DEFAULT);
            GetMappingsResponse destIndexMapping =
                restClient.indices().getMapping(new GetMappingsRequest().indices(destIndexName), RequestOptions.DEFAULT);
            assertThat(
                destIndexMapping.mappings().get(destIndexName).sourceAsMap().get("properties"),
                is(equalTo(sourceIndexMapping.mappings().get(SOURCE_INDEX_NAME).sourceAsMap().get("properties"))));
            // Verify destination index contents
            SearchResponse searchResponse =
                restClient.search(new SearchRequest(destIndexName).source(new SearchSourceBuilder().size(1000)), RequestOptions.DEFAULT);
            assertThat(searchResponse.getHits().getTotalHits().value, is(equalTo(Long.valueOf(NUM_USERS))));
            assertThat(
                Stream.of(searchResponse.getHits().getHits())
                    .map(SearchHit::getSourceAsMap)
                    .peek(doc -> doc.keySet().removeIf(k -> k.startsWith("_")))
                    .collect(toList()),
                containsInAnyOrder(EXPECTED_DEST_INDEX_ROWS));
        }
    }

    public void testLatestDocPreview() throws Exception {
        createReviewsIndex(SOURCE_INDEX_NAME, 100, NUM_USERS, LatestDocIT::getDateStringForRow);

        TransformConfig transformConfig =
            createTransformConfigBuilder(TRANSFORM_NAME, "dummy", SOURCE_INDEX_NAME)
                .setLatestDocConfig(
                    LatestDocConfig.builder()
                        .setUniqueKey(USER_ID)
                        .setSort(SortBuilders.fieldSort(TIMESTAMP).order(SortOrder.DESC))
                        .build())
                .build();

        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            PreviewTransformResponse previewResponse =
                restClient.transform().previewTransform(new PreviewTransformRequest(transformConfig), RequestOptions.DEFAULT);
            // Verify preview mappings
            GetMappingsResponse sourceIndexMapping =
                restClient.indices().getMapping(new GetMappingsRequest().indices(SOURCE_INDEX_NAME), RequestOptions.DEFAULT);
            assertThat(
                previewResponse.getMappings().get("properties"),
                is(equalTo(sourceIndexMapping.mappings().get(SOURCE_INDEX_NAME).sourceAsMap().get("properties"))));
            // Verify preview contents
            assertThat(previewResponse.getDocs(), hasSize(NUM_USERS));
            assertThat(previewResponse.getDocs(), containsInAnyOrder(EXPECTED_DEST_INDEX_ROWS));
        }
    }
}
