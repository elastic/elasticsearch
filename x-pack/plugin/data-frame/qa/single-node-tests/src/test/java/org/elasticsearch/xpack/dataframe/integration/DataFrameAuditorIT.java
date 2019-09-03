/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class DataFrameAuditorIT extends DataFrameRestTestCase {

    private static final String TEST_USER_NAME = "df_admin_plus_data";
    private static final String DATA_ACCESS_ROLE = "test_data_access";
    private static final String BASIC_AUTH_VALUE_DATA_FRAME_ADMIN_WITH_SOME_DATA_ACCESS =
        basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING);

    private static boolean indicesCreated = false;

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndexes() throws IOException {

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

        createReviewsIndex();
        indicesCreated = true;
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME);
        setupUser(TEST_USER_NAME, Arrays.asList("data_frame_transforms_admin", DATA_ACCESS_ROLE));
    }

    @SuppressWarnings("unchecked")
    public void testAuditorWritesAudits() throws Exception {
        String transformId = "simple_pivot_for_audit";
        String dataFrameIndex = "pivot_reviews_user_id_above_20";
        setupDataAccessRole(DATA_ACCESS_ROLE, REVIEWS_INDEX_NAME, dataFrameIndex);
        String query = "\"match\": {\"user_id\": \"user_26\"}";

        createPivotReviewsTransform(transformId, dataFrameIndex, query, null, BASIC_AUTH_VALUE_DATA_FRAME_ADMIN_WITH_SOME_DATA_ACCESS);

        startAndWaitForTransform(transformId, dataFrameIndex, BASIC_AUTH_VALUE_DATA_FRAME_ADMIN_WITH_SOME_DATA_ACCESS);

        // Make sure we wrote to the audit
        final Request request = new Request("GET", DataFrameInternalIndex.AUDIT_INDEX + "/_search");
        request.setJsonEntity("{\"query\":{\"term\":{\"transform_id\":\"simple_pivot_for_audit\"}}}");
        assertBusy(() -> {
            assertTrue(indexExists(DataFrameInternalIndex.AUDIT_INDEX));
        });
        // Since calls to write the AbstractAuditor are sent and forgot (async) we could have returned from the start,
        // finished the job (as this is a very short DF job), all without the audit being fully written.
        assertBusy(() -> {
            refreshIndex(DataFrameInternalIndex.AUDIT_INDEX);
            Map<String, Object> response = entityAsMap(client().performRequest(request));
            List<?> hitList = ((List<?>) ((Map<?, ?>)response.get("hits")).get("hits"));
            assertThat(hitList, is(not(empty())));
            Map<?, ?> hitRsp = (Map<?, ?>) hitList.get(0);
            Map<String, Object> source = (Map<String, Object>)hitRsp.get("_source");
            assertThat(source.get("transform_id"), equalTo(transformId));
            assertThat(source.get("level"), equalTo("info"));
            assertThat(source.get("message"), is(notNullValue()));
            assertThat(source.get("node_name"), is(notNullValue()));
            assertThat(source.get("timestamp"), is(notNullValue()));
        });

    }
}
