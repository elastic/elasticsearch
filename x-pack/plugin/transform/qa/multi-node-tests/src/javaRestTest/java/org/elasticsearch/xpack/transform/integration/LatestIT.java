/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.junit.After;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class LatestIT extends TransformRestTestCase {

    private static final String SOURCE_INDEX_NAME = "basic-crud-latest-reviews";
    private static final int NUM_USERS = 28;

    private static final String TRANSFORM_NAME = "transform-crud-latest";

    private static Integer getUserIdForRow(int row) {
        int userId = row % (NUM_USERS + 1);
        return userId < NUM_USERS ? userId : null;
    }

    private static String getDateStringForRow(int row) {
        int month = 1 + (row / 28);
        int day = 1 + (row % 28);
        return "2017-" + (month < 10 ? "0" + month : month) + "-" + (day < 10 ? "0" + day : day) + "T12:30:00Z";
    }

    private static final String TIMESTAMP = "timestamp";
    private static final String USER_ID = "user_id";
    private static final String BUSINESS_ID = "business_id";
    private static final String COUNT = "count";
    private static final String STARS = "stars";
    private static final String COMMENT = "comment";

    private static Map<String, Object> row(String userId, String businessId, int count, int stars, String timestamp, String comment) {
        return new HashMap<>() {
            {
                if (userId != null) {
                    put(USER_ID, userId);
                }
                put(BUSINESS_ID, businessId);
                put(COUNT, count);
                put(STARS, stars);
                put(TIMESTAMP, timestamp);
                put(COMMENT, comment);
                put("regular_object", singletonMap("foo", 42));
                put("nested_object", singletonMap("bar", 43));
            }
        };
    }

    private static final Object[] EXPECTED_DEST_INDEX_ROWS = new Object[] {
        row("user_0", "business_37", 87, 2, "2017-04-04T12:30:00Z", "Great stuff, deserves 2 stars"),
        row("user_1", "business_38", 88, 3, "2017-04-05T12:30:00Z", "Great stuff, deserves 3 stars"),
        row("user_2", "business_39", 89, 4, "2017-04-06T12:30:00Z", "Great stuff, deserves 4 stars"),
        row("user_3", "business_40", 90, 0, "2017-04-07T12:30:00Z", "Great stuff, deserves 0 stars"),
        row("user_4", "business_41", 91, 1, "2017-04-08T12:30:00Z", "Great stuff, deserves 1 stars"),
        row("user_5", "business_42", 92, 2, "2017-04-09T12:30:00Z", "Great stuff, deserves 2 stars"),
        row("user_6", "business_43", 93, 3, "2017-04-10T12:30:00Z", "Great stuff, deserves 3 stars"),
        row("user_7", "business_44", 94, 4, "2017-04-11T12:30:00Z", "Great stuff, deserves 4 stars"),
        row("user_8", "business_45", 95, 0, "2017-04-12T12:30:00Z", "Great stuff, deserves 0 stars"),
        row("user_9", "business_46", 96, 1, "2017-04-13T12:30:00Z", "Great stuff, deserves 1 stars"),
        row("user_10", "business_47", 97, 2, "2017-04-14T12:30:00Z", "Great stuff, deserves 2 stars"),
        row("user_11", "business_48", 98, 3, "2017-04-15T12:30:00Z", "Great stuff, deserves 3 stars"),
        row("user_12", "business_49", 99, 4, "2017-04-16T12:30:00Z", "Great stuff, deserves 4 stars"),
        row("user_13", "business_21", 71, 1, "2017-03-16T12:30:00Z", "Great stuff, deserves 1 stars"),
        row("user_14", "business_22", 72, 2, "2017-03-17T12:30:00Z", "Great stuff, deserves 2 stars"),
        row("user_15", "business_23", 73, 3, "2017-03-18T12:30:00Z", "Great stuff, deserves 3 stars"),
        row("user_16", "business_24", 74, 4, "2017-03-19T12:30:00Z", "Great stuff, deserves 4 stars"),
        row("user_17", "business_25", 75, 0, "2017-03-20T12:30:00Z", "Great stuff, deserves 0 stars"),
        row("user_18", "business_26", 76, 1, "2017-03-21T12:30:00Z", "Great stuff, deserves 1 stars"),
        row("user_19", "business_27", 77, 2, "2017-03-22T12:30:00Z", "Great stuff, deserves 2 stars"),
        row("user_20", "business_28", 78, 3, "2017-03-23T12:30:00Z", "Great stuff, deserves 3 stars"),
        row("user_21", "business_29", 79, 4, "2017-03-24T12:30:00Z", "Great stuff, deserves 4 stars"),
        row("user_22", "business_30", 80, 0, "2017-03-25T12:30:00Z", "Great stuff, deserves 0 stars"),
        row("user_23", "business_31", 81, 1, "2017-03-26T12:30:00Z", "Great stuff, deserves 1 stars"),
        row("user_24", "business_32", 82, 2, "2017-03-27T12:30:00Z", "Great stuff, deserves 2 stars"),
        row("user_25", "business_33", 83, 3, "2017-03-28T12:30:00Z", "Great stuff, deserves 3 stars"),
        row("user_26", "business_34", 84, 4, "2017-04-01T12:30:00Z", "Great stuff, deserves 4 stars"),
        row("user_27", "business_35", 85, 0, "2017-04-02T12:30:00Z", "Great stuff, deserves 0 stars"),
        row(null, "business_36", 86, 1, "2017-04-03T12:30:00Z", "Great stuff, deserves 1 stars") };

    @After
    public void cleanTransforms() throws Exception {
        cleanUp();
    }

    @SuppressWarnings("unchecked")
    public void testLatest() throws Exception {
        createReviewsIndex(SOURCE_INDEX_NAME, 100, NUM_USERS, LatestIT::getUserIdForRow, LatestIT::getDateStringForRow);

        String destIndexName = "reviews-latest";
        TransformConfig transformConfig = createTransformConfigBuilder(
            TRANSFORM_NAME,
            destIndexName,
            QueryConfig.matchAll(),
            SOURCE_INDEX_NAME
        ).setLatestConfig(new LatestConfig(List.of(USER_ID), TIMESTAMP)).build();
        putTransform(TRANSFORM_NAME, Strings.toString(transformConfig), RequestOptions.DEFAULT);
        startTransform(transformConfig.getId(), RequestOptions.DEFAULT);
        waitUntilCheckpoint(transformConfig.getId(), 1L);
        stopTransform(transformConfig.getId());

        refreshIndex(destIndexName, RequestOptions.DEFAULT);
        var mappings = getIndexMapping(destIndexName, RequestOptions.DEFAULT);
        assertThat(
            (Map<String, Object>) XContentMapValues.extractValue(destIndexName + ".mappings", mappings),
            allOf(hasKey("_meta"), hasKey("properties"))
        );
        var searchResponse = search(destIndexName, 1000, RequestOptions.DEFAULT);
        assertThat((Integer) XContentMapValues.extractValue("hits.total.value", searchResponse), is(equalTo(NUM_USERS + 1)));
        var hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", searchResponse);
        var searchedDocs = hits.stream().map(h -> (Map<String, Object>) h.get("_source")).collect(Collectors.toList());
        assertThat(searchedDocs, containsInAnyOrder(EXPECTED_DEST_INDEX_ROWS));
    }

    private Map<String, Object> search(String index, int size, RequestOptions options) throws IOException {
        var r = new Request("GET", index + "/_search?size=" + size);
        r.setOptions(options);
        return entityAsMap(client().performRequest(r));
    }

    @SuppressWarnings("unchecked")
    public void testLatestPreview() throws Exception {
        createReviewsIndex(SOURCE_INDEX_NAME, 100, NUM_USERS, LatestIT::getUserIdForRow, LatestIT::getDateStringForRow);

        TransformConfig transformConfig = createTransformConfigBuilder(TRANSFORM_NAME, "dummy", QueryConfig.matchAll(), SOURCE_INDEX_NAME)
            .setLatestConfig(new LatestConfig(List.of(USER_ID), TIMESTAMP))
            .build();

        var previewResponse = previewTransform(Strings.toString(transformConfig), RequestOptions.DEFAULT);
        // Verify preview mappings
        var mappings = (Map<String, Object>) XContentMapValues.extractValue("generated_dest_index.mappings", previewResponse);
        assertThat(mappings, allOf(hasKey("_meta"), hasEntry("properties", emptyMap())));
        // Verify preview contents
        var docs = (List<Map<String, Object>>) XContentMapValues.extractValue("preview", previewResponse);
        assertThat(docs, hasSize(NUM_USERS + 1));
        assertThat(docs, containsInAnyOrder(EXPECTED_DEST_INDEX_ROWS));
    }
}
