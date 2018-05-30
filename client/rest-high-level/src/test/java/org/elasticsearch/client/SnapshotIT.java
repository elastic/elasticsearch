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

package org.elasticsearch.client;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class SnapshotIT extends ESRestHighLevelClientTestCase {

    private PutRepositoryResponse createTestRepository(String repository, String type, String settings) throws IOException {
        PutRepositoryRequest request = new PutRepositoryRequest(repository);
        request.settings(settings, XContentType.JSON);
        request.type(type);
        return execute(request, highLevelClient().snapshot()::createRepository,
            highLevelClient().snapshot()::createRepositoryAsync);
    }

    public void testCreateRepository() throws IOException {
        PutRepositoryResponse response = createTestRepository("test", FsRepository.TYPE, "{\"location\": \".\"}");
        assertTrue(response.isAcknowledged());
    }

    public void testSnapshotGetRepositoriesUsingParams() throws IOException {
        String testRepository = "test";
        assertTrue(createTestRepository(testRepository, FsRepository.TYPE, "{\"location\": \".\"}").isAcknowledged());
        assertTrue(createTestRepository("other", FsRepository.TYPE, "{\"location\": \".\"}").isAcknowledged());

        GetRepositoriesRequest request = new GetRepositoriesRequest();
        request.repositories(new String[]{testRepository});
        GetRepositoriesResponse response = execute(request, highLevelClient().snapshot()::getRepositories,
            highLevelClient().snapshot()::getRepositoriesAsync);
        assertThat(1, equalTo(response.repositories().size()));
    }

    public void testSnapshotGetDefaultRepositories() throws IOException {
        assertTrue(createTestRepository("other", FsRepository.TYPE, "{\"location\": \".\"}").isAcknowledged());
        assertTrue(createTestRepository("test", FsRepository.TYPE, "{\"location\": \".\"}").isAcknowledged());

        GetRepositoriesResponse response = execute(new GetRepositoriesRequest(), highLevelClient().snapshot()::getRepositories,
            highLevelClient().snapshot()::getRepositoriesAsync);
        assertThat(2, equalTo(response.repositories().size()));
    }

    public void testSnapshotGetRepositoriesNonExistent() {
        String repository = "doesnotexist";
        GetRepositoriesRequest request = new GetRepositoriesRequest(new String[]{repository});
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> execute(request,
            highLevelClient().snapshot()::getRepositories, highLevelClient().snapshot()::getRepositoriesAsync));

        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(exception.getMessage(), equalTo(
            "Elasticsearch exception [type=repository_missing_exception, reason=[" + repository + "] missing]"));
    }

    public void testSnapshotDeleteRepository() throws IOException {
        String repository = "test";
        String repositorySettings = "{\"type\":\"fs\", \"settings\":{\"location\": \".\"}}";

        highLevelClient().getLowLevelClient().performRequest("put", "_snapshot/" + repository,
            Collections.emptyMap(), new StringEntity(repositorySettings, ContentType.APPLICATION_JSON));

        GetRepositoriesRequest request = new GetRepositoriesRequest();
        GetRepositoriesResponse response = execute(request, highLevelClient().snapshot()::getRepositories,
            highLevelClient().snapshot()::getRepositoriesAsync);
        assertThat(1, equalTo(response.repositories().size()));

        DeleteRepositoryRequest deleteRequest = new DeleteRepositoryRequest(repository);
        DeleteRepositoryResponse deleteResponse = execute(deleteRequest, highLevelClient().snapshot()::deleteRepository,
            highLevelClient().snapshot()::deleteRepositoryAsync);

        assertTrue(deleteResponse.isAcknowledged());
    }
}
