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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ModulesClientIT extends ESRestHighLevelClientTestCase {

    public void testModulesGetRepositories() throws IOException {
        GetRepositoriesRequest request = new GetRepositoriesRequest();
        GetRepositoriesResponse response = execute(request, highLevelClient().modules()::getRepositories,
            highLevelClient().modules()::getRepositoriesAsync);
        // Once create repository is fleshed out, we will be able to do more with this test case.
        assertThat(0, equalTo(response.repositories().size()));
    }

    public void testModulesGetRepositoriesNonExistent() throws IOException {
        String repo = "doesnotexist";
        GetRepositoriesRequest request = new GetRepositoriesRequest(new String[]{repo});
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> execute(request,
            highLevelClient().modules()::getRepositories, highLevelClient().modules()::getRepositoriesAsync));

        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(exception.getMessage(), equalTo(
            "Elasticsearch exception [type=repository_missing_exception, reason=[" + repo + "] missing]"));
    }
}
