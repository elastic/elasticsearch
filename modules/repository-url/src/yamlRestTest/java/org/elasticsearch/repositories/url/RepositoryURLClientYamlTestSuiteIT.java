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

package org.elasticsearch.repositories.url;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class RepositoryURLClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public RepositoryURLClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    /**
     * This method registers 3 snapshot/restore repositories:
     * - repository-fs: this FS repository is used to create snapshots.
     * - repository-url: this URL repository is used to restore snapshots created using the previous repository. It uses
     * the URLFixture to restore snapshots over HTTP.
     * - repository-file: similar as the previous repository but using a file:// prefix instead of http://.
     **/
    @Before
    public void registerRepositories() throws IOException {
        Request clusterSettingsRequest = new Request("GET", "/_cluster/settings");
        clusterSettingsRequest.addParameter("include_defaults", "true");
        clusterSettingsRequest.addParameter("filter_path", "defaults.path.repo,defaults.repositories.url.allowed_urls");
        Response clusterSettingsResponse = client().performRequest(clusterSettingsRequest);
        Map<String, Object> clusterSettings = entityAsMap(clusterSettingsResponse);

        @SuppressWarnings("unchecked")
        List<String> pathRepos = (List<String>) XContentMapValues.extractValue("defaults.path.repo", clusterSettings);
        assertThat(pathRepos, notNullValue());
        assertThat(pathRepos, hasSize(1));

        final String pathRepo = pathRepos.get(0);
        final URI pathRepoUri = PathUtils.get(pathRepo).toUri().normalize();

        // Create a FS repository using the path.repo location
        Request createFsRepositoryRequest = new Request("PUT", "/_snapshot/repository-fs");
        createFsRepositoryRequest.setEntity(buildRepositorySettings(FsRepository.TYPE,
                Settings.builder().put("location", pathRepo).build()));
        Response createFsRepositoryResponse = client().performRequest(createFsRepositoryRequest);
        assertThat(createFsRepositoryResponse.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));

        // Create a URL repository using the file://{path.repo} URL
        Request createFileRepositoryRequest = new Request("PUT", "/_snapshot/repository-file");
        createFileRepositoryRequest.setEntity(buildRepositorySettings("url",
                Settings.builder().put("url", pathRepoUri.toString()).build()));
        Response createFileRepositoryResponse = client().performRequest(createFileRepositoryRequest);
        assertThat(createFileRepositoryResponse.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));

        // Create a URL repository using the http://{fixture} URL
        @SuppressWarnings("unchecked")
        List<String> allowedUrls = (List<String>) XContentMapValues.extractValue("defaults.repositories.url.allowed_urls", clusterSettings);
        for (String allowedUrl : allowedUrls) {
            try {
                InetAddress inetAddress = InetAddress.getByName(new URL(allowedUrl).getHost());
                if (inetAddress.isAnyLocalAddress() || inetAddress.isLoopbackAddress()) {
                    Request createUrlRepositoryRequest = new Request("PUT", "/_snapshot/repository-url");
                    createUrlRepositoryRequest.setEntity(buildRepositorySettings("url",
                            Settings.builder().put("url", allowedUrl).build()));
                    Response createUrlRepositoryResponse = client().performRequest(createUrlRepositoryRequest);
                    assertThat(createUrlRepositoryResponse.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
                    break;
                }
            } catch (Exception e) {
                logger.debug("Failed to resolve inet address for allowed URL [{}], skipping", allowedUrl);
            }
        }
    }

    private static HttpEntity buildRepositorySettings(final String type, final Settings settings) throws IOException {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.field("type", type);
                builder.startObject("settings");
                {
                    settings.toXContent(builder, ToXContent.EMPTY_PARAMS);
                }
                builder.endObject();
            }
            builder.endObject();
            return new NStringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
        }
    }
}
