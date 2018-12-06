/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.documentation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.migration.IndexUpgradeInfoRequest;
import org.elasticsearch.client.migration.IndexUpgradeInfoResponse;
import org.elasticsearch.client.migration.IndexUpgradeRequest;
import org.elasticsearch.client.migration.UpgradeActionRequired;
import org.elasticsearch.client.tasks.TaskSubmissionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

/**
 * This class is used to generate the Java Migration API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example
 * // end::example
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/MigrationClientDocumentationIT.java[example]
 * --------------------------------------------------
 *
 * The column width of the code block is 84. If the code contains a line longer
 * than 84, the line will be cut and a horizontal scroll bar will be displayed.
 * (the code indentation of the tag is not included in the width)
 */
public class MigrationClientDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testGetAssistance() throws IOException {
        RestHighLevelClient client = highLevelClient();

        // tag::get-assistance-request
        IndexUpgradeInfoRequest request = new IndexUpgradeInfoRequest(); // <1>
        // end::get-assistance-request

        // tag::get-assistance-request-indices
        request.indices("index1", "index2"); // <1>
        // end::get-assistance-request-indices

        request.indices(Strings.EMPTY_ARRAY);

        // tag::get-assistance-request-indices-options
        request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
        // end::get-assistance-request-indices-options

        // tag::get-assistance-execute
        IndexUpgradeInfoResponse response = client.migration().getAssistance(request, RequestOptions.DEFAULT);
        // end::get-assistance-execute

        // tag::get-assistance-response
        Map<String, UpgradeActionRequired> actions = response.getActions();
        for (Map.Entry<String, UpgradeActionRequired> entry : actions.entrySet()) {
            String index = entry.getKey(); // <1>
            UpgradeActionRequired actionRequired = entry.getValue(); // <2>
        }
        // end::get-assistance-response
    }

    public void testUpgrade() throws IOException {

        RestHighLevelClient client = highLevelClient();
        createIndex("test", Settings.EMPTY);


        // tag::upgrade-request
        IndexUpgradeRequest request = new IndexUpgradeRequest("test"); // <1>
        // end::upgrade-request

        try {

            // tag::upgrade-execute
            BulkByScrollResponse response = client.migration().upgrade(request, RequestOptions.DEFAULT);
            // end::upgrade-execute

        } catch (ElasticsearchStatusException e) {
            assertThat(e.getMessage(), containsString("cannot be upgraded"));
        }
    }

    public void testUpgradeAsync() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();
        createIndex("test", Settings.EMPTY);
        final CountDownLatch latch = new CountDownLatch(1);

        // tag::upgrade-async-listener
        ActionListener<BulkByScrollResponse> listener = new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkResponse) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::upgrade-async-listener

        listener = new LatchedActionListener<>(listener, latch);

        // tag::upgrade-async-execute
        client.migration().upgradeAsync(new IndexUpgradeRequest("test"), RequestOptions.DEFAULT, listener); // <1>
        // end::upgrade-async-execute

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testUpgradeWithTaskApi() throws IOException {
        createIndex("test", Settings.EMPTY);
        RestHighLevelClient client = highLevelClient();
        // tag::upgrade-task-api
        IndexUpgradeRequest request = new IndexUpgradeRequest("test");

        TaskSubmissionResponse response = client.migration()
            .submitUpgradeTask(request, RequestOptions.DEFAULT);
        String taskId = response.getTask();
        // end::upgrade-task-api
        assertThat(taskId, not(isEmptyOrNullString()));
    }
}
