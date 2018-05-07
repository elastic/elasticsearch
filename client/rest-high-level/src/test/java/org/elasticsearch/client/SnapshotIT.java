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
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class SnapshotIT extends ESRestHighLevelClientTestCase {

    public void testModulesGetRepositoriesUsingParams() throws IOException {
        String repository = "test";
        String repositorySettings = "{\"type\":\"fs\", \"settings\":{\"location\": \".\"}}";
        highLevelClient().getLowLevelClient().performRequest("put", "_snapshot/" + repository, Collections.emptyMap(),
            new StringEntity(repositorySettings, ContentType.APPLICATION_JSON));

        highLevelClient().getLowLevelClient().performRequest("put", "_snapshot/" + repository + "_other", Collections.emptyMap(),
            new StringEntity(repositorySettings, ContentType.APPLICATION_JSON));

        {
            GetRepositoriesRequest request = new GetRepositoriesRequest();
            request.repositories(new String[]{repository});
            GetRepositoriesResponse response = execute(request, highLevelClient().snapshot()::getRepositories,
                highLevelClient().snapshot()::getRepositoriesAsync);
            assertThat(1, equalTo(response.repositories().size()));
        }
        {
            GetRepositoriesRequest request = new GetRepositoriesRequest();
            GetRepositoriesResponse response = execute(request, highLevelClient().snapshot()::getRepositories,
                highLevelClient().snapshot()::getRepositoriesAsync);
            assertThat(2, equalTo(response.repositories().size()));
        }
    }

    public void testModulesGetDefaultRepositories() throws IOException {
        String repositorySettings = "{\"type\":\"fs\", \"settings\":{\"location\": \".\"}}";
        GetRepositoriesRequest request = new GetRepositoriesRequest();

        highLevelClient().getLowLevelClient().performRequest("put", "_snapshot/test", Collections.emptyMap(),
        new StringEntity(repositorySettings, ContentType.APPLICATION_JSON));

        GetRepositoriesResponse response = execute(request, highLevelClient().snapshot()::getRepositories,
            highLevelClient().snapshot()::getRepositoriesAsync);
        assertThat(1, equalTo(response.repositories().size()));
    }

    public void testModulesGetRepositoriesNonExistent() throws IOException {
        String repository = "doesnotexist";
        GetRepositoriesRequest request = new GetRepositoriesRequest(new String[]{repository});
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> execute(request,
            highLevelClient().snapshot()::getRepositories, highLevelClient().snapshot()::getRepositoriesAsync));

        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(exception.getMessage(), equalTo(
            "Elasticsearch exception [type=repository_missing_exception, reason=[" + repository + "] missing]"));
    }
}
