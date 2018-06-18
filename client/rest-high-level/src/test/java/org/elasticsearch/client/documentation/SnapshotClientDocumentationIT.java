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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.repositories.fs.FsRepository;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * This class is used to generate the Java Snapshot API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example
 * // end::example
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/SnapshotClientDocumentationIT.java[example]
 * --------------------------------------------------
 *
 * The column width of the code block is 84. If the code contains a line longer
 * than 84, the line will be cut and a horizontal scroll bar will be displayed.
 * (the code indentation of the tag is not included in the width)
 */
public class SnapshotClientDocumentationIT extends ESRestHighLevelClientTestCase {

    private static final String repositoryName = "test_repository";

    public void testSnapshotCreateRepository() throws IOException {
        RestHighLevelClient client = highLevelClient();

        // tag::create-repository-request
        PutRepositoryRequest request = new PutRepositoryRequest();
        // end::create-repository-request

        // tag::create-repository-create-settings
        String locationKey = FsRepository.LOCATION_SETTING.getKey();
        String locationValue = ".";
        String compressKey = FsRepository.COMPRESS_SETTING.getKey();
        boolean compressValue = true;

        Settings settings = Settings.builder()
            .put(locationKey, locationValue)
            .put(compressKey, compressValue)
            .build(); // <1>
        // end::create-repository-create-settings

        // tag::create-repository-request-repository-settings
        request.settings(settings); // <1>
        // end::create-repository-request-repository-settings

        {
            // tag::create-repository-settings-builder
            Settings.Builder settingsBuilder = Settings.builder()
                .put(locationKey, locationValue)
                .put(compressKey, compressValue);
            request.settings(settingsBuilder); // <1>
            // end::create-repository-settings-builder
        }
        {
            // tag::create-repository-settings-map
            Map<String, Object> map = new HashMap<>();
            map.put(locationKey, locationValue);
            map.put(compressKey, compressValue);
            request.settings(map); // <1>
            // end::create-repository-settings-map
        }
        {
            // tag::create-repository-settings-source
            request.settings("{\"location\": \".\", \"compress\": \"true\"}",
                XContentType.JSON); // <1>
            // end::create-repository-settings-source
        }

        // tag::create-repository-request-name
        request.name(repositoryName); // <1>
        // end::create-repository-request-name
        // tag::create-repository-request-type
        request.type(FsRepository.TYPE); // <1>
        // end::create-repository-request-type

        // tag::create-repository-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::create-repository-request-masterTimeout
        // tag::create-repository-request-timeout
        request.timeout(TimeValue.timeValueMinutes(1)); // <1>
        request.timeout("1m"); // <2>
        // end::create-repository-request-timeout
        // tag::create-repository-request-verify
        request.verify(true); // <1>
        // end::create-repository-request-verify

        // tag::create-repository-execute
        PutRepositoryResponse response = client.snapshot().createRepository(request, RequestOptions.DEFAULT);
        // end::create-repository-execute

        // tag::create-repository-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::create-repository-response
        assertTrue(acknowledged);
    }

    public void testSnapshotCreateRepositoryAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            PutRepositoryRequest request = new PutRepositoryRequest(repositoryName);

            // tag::create-repository-execute-listener
            ActionListener<PutRepositoryResponse> listener =
                new ActionListener<PutRepositoryResponse>() {
                    @Override
                    public void onResponse(PutRepositoryResponse putRepositoryResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::create-repository-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::create-repository-execute-async
            client.snapshot().createRepositoryAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::create-repository-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSnapshotGetRepository() throws IOException {
        RestHighLevelClient client = highLevelClient();

        createTestRepositories();

        // tag::get-repository-request
        GetRepositoriesRequest request = new GetRepositoriesRequest();
        // end::get-repository-request

        // tag::get-repository-request-repositories
        String [] repositories = new String[] {repositoryName};
        request.repositories(repositories); // <1>
        // end::get-repository-request-repositories
        // tag::get-repository-request-local
        request.local(true); // <1>
        // end::get-repository-request-local
        // tag::get-repository-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::get-repository-request-masterTimeout

        // tag::get-repository-execute
        GetRepositoriesResponse response = client.snapshot().getRepositories(request, RequestOptions.DEFAULT);
        // end::get-repository-execute

        // tag::get-repository-response
        List<RepositoryMetaData> repositoryMetaDataResponse = response.repositories();
        // end::get-repository-response
        assertThat(1, equalTo(repositoryMetaDataResponse.size()));
        assertThat(repositoryName, equalTo(repositoryMetaDataResponse.get(0).name()));
    }

    public void testSnapshotGetRepositoryAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            GetRepositoriesRequest request = new GetRepositoriesRequest();

            // tag::get-repository-execute-listener
            ActionListener<GetRepositoriesResponse> listener =
                    new ActionListener<GetRepositoriesResponse>() {
                @Override
                public void onResponse(GetRepositoriesResponse getRepositoriesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-repository-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-repository-execute-async
            client.snapshot().getRepositoriesAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-repository-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSnapshotDeleteRepository() throws IOException {
        RestHighLevelClient client = highLevelClient();

        createTestRepositories();

        // tag::delete-repository-request
        DeleteRepositoryRequest request = new DeleteRepositoryRequest(repositoryName);
        // end::delete-repository-request

        // tag::delete-repository-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::delete-repository-request-masterTimeout
        // tag::delete-repository-request-timeout
        request.timeout(TimeValue.timeValueMinutes(1)); // <1>
        request.timeout("1m"); // <2>
        // end::delete-repository-request-timeout

        // tag::delete-repository-execute
        DeleteRepositoryResponse response = client.snapshot().deleteRepository(request, RequestOptions.DEFAULT);
        // end::delete-repository-execute

        // tag::delete-repository-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::delete-repository-response
        assertTrue(acknowledged);
    }

    public void testSnapshotDeleteRepositoryAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            DeleteRepositoryRequest request = new DeleteRepositoryRequest();

            // tag::delete-repository-execute-listener
            ActionListener<DeleteRepositoryResponse> listener =
                new ActionListener<DeleteRepositoryResponse>() {
                    @Override
                    public void onResponse(DeleteRepositoryResponse deleteRepositoryResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::delete-repository-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-repository-execute-async
            client.snapshot().deleteRepositoryAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-repository-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSnapshotVerifyRepository() throws IOException {
        RestHighLevelClient client = highLevelClient();
        createTestRepositories();

        // tag::verify-repository-request
        VerifyRepositoryRequest request = new VerifyRepositoryRequest(repositoryName);
        // end::verify-repository-request

        // tag::verify-repository-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::verify-repository-request-masterTimeout
        // tag::verify-repository-request-timeout
        request.timeout(TimeValue.timeValueMinutes(1)); // <1>
        request.timeout("1m"); // <2>
        // end::verify-repository-request-timeout

        // tag::verify-repository-execute
        VerifyRepositoryResponse response = client.snapshot().verifyRepository(request, RequestOptions.DEFAULT);
        // end::verify-repository-execute

        // tag::verify-repository-response
        List<VerifyRepositoryResponse.NodeView> repositoryMetaDataResponse = response.getNodes();
        // end::verify-repository-response
        assertThat(1, equalTo(repositoryMetaDataResponse.size()));
        assertThat("node-0", equalTo(repositoryMetaDataResponse.get(0).getName()));
    }

    public void testSnapshotVerifyRepositoryAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            VerifyRepositoryRequest request = new VerifyRepositoryRequest(repositoryName);

            // tag::verify-repository-execute-listener
            ActionListener<VerifyRepositoryResponse> listener =
                new ActionListener<VerifyRepositoryResponse>() {
                    @Override
                    public void onResponse(VerifyRepositoryResponse verifyRepositoryRestResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::verify-repository-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::verify-repository-execute-async
            client.snapshot().verifyRepositoryAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::verify-repository-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    private void createTestRepositories() throws IOException {
        PutRepositoryRequest request = new PutRepositoryRequest(repositoryName);
        request.type(FsRepository.TYPE);
        request.settings("{\"location\": \".\"}", XContentType.JSON);
        assertTrue(highLevelClient().snapshot().createRepository(request, RequestOptions.DEFAULT).isAcknowledged());
    }
}
