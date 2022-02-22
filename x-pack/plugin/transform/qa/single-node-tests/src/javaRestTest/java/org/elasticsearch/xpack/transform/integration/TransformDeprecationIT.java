/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class TransformDeprecationIT extends TransformRestTestCase {

    private static final String TEST_USER_NAME = "transform_admin_plus_data";
    private static final String DATA_ACCESS_ROLE = "test_data_access";

    private static boolean indicesCreated = false;

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndexes() throws IOException {
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        setupUser(TEST_USER_NAME, Arrays.asList("transform_admin", DATA_ACCESS_ROLE));

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }
        createReviewsIndex();
        indicesCreated = true;
    }

    public void testDeprecationInfo() throws Exception {
        {
            Request deprecationInfoRequest = new Request("GET", "_migration/deprecations");
            Response deprecationInfoResponse = client().performRequest(deprecationInfoRequest);
            assertThat(EntityUtils.toString(deprecationInfoResponse.getEntity()), not(containsString("Use of the joda time method")));
        }
        {
            String transformId = "script_deprecated_syntax";
            Request createTransformRequest = new Request("PUT", getTransformEndpoint() + transformId);
            // We need this as creation of transform with deprecated script syntax triggers warnings
            createTransformRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            String config = createTransformConfig(REVIEWS_INDEX_NAME, transformId);
            createTransformRequest.setJsonEntity(config);
            client().performRequest(createTransformRequest);
        }
        {
            Request deprecationInfoRequest = new Request("GET", "_migration/deprecations");
            deprecationInfoRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            Response deprecationInfoResponse = client().performRequest(deprecationInfoRequest);
            assertThat(
                EntityUtils.toString(deprecationInfoResponse.getEntity()),
                allOf(
                    containsString("Use of the joda time method [getMillis()] is deprecated. Use [toInstant().toEpochMilli()] instead."),
                    containsString("Use of the joda time method [getEra()] is deprecated. Use [get(ChronoField.ERA)] instead.")
                )
            );
        }
    }

    private static String createTransformConfig(String sourceIndex, String destinationIndex) {
        return "{"
            + "  \"source\": {"
            + "    \"index\": \""
            + sourceIndex
            + "\","
            + "    \"runtime_mappings\": {"
            + "      \"timestamp-5m\": {"
            + "        \"type\": \"date\","
            + "        \"script\": {"
            // We don't use "era" for anything in this script. This is solely to generate the deprecation warning.
            + "          \"source\": \"def era = doc['timestamp'].value.era; emit(doc['timestamp'].value.millis)\""
            + "        }"
            + "      }"
            + "    }"
            + "  },"
            + "  \"dest\": {"
            + "    \"index\": \""
            + destinationIndex
            + "\""
            + "  },"
            + "  \"pivot\": {"
            + "    \"group_by\": {"
            + "      \"timestamp\": {"
            + "        \"date_histogram\": {"
            + "          \"field\": \"timestamp-5m\","
            + "          \"calendar_interval\": \"1m\""
            + "        }"
            + "      }"
            + "    },"
            + "    \"aggregations\": {"
            + "      \"bytes.avg\": {"
            + "        \"avg\": {"
            + "          \"field\": \"bytes\""
            + "        }"
            + "      },"
            + "      \"millis\": {"
            + "        \"scripted_metric\": {"
            + "          \"init_script\": \"state.m = 0\","
            + "          \"map_script\": \"state.m = doc['timestamp'].value.millis;\","
            + "          \"combine_script\": \"return state.m;\","
            + "          \"reduce_script\": \"def last = 0; for (s in states) {last = s;} return last;\""
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}";
    }
}
