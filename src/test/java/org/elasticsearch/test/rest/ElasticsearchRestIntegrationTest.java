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

package org.elasticsearch.test.rest;

import com.google.common.collect.Lists;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.rest.client.RestClient;
import org.elasticsearch.test.rest.client.RestException;
import org.elasticsearch.test.rest.spec.RestSpec;
import org.elasticsearch.test.rest.support.PathUtils;

import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * {@link ElasticsearchRestIntegrationTest} is an abstract base class for integration tests
 * that require the ability to talk to a JVM private Elasticsearch cluster via the REST API.
 *
 * This class add a {@link org.elasticsearch.test.rest.client.RestClient} for use in testing
 * the REST API.
 *
 * {@see ElasticsearchIntegrationTest}
 */
public abstract class ElasticsearchRestIntegrationTest extends ElasticsearchIntegrationTest {

    private static final String DEFAULT_SPEC_PATH = "/rest-api-spec/api";
    private static final String REST_TESTS_SPEC = "tests.rest.spec";

    protected RestClient restClient;

    @Before
    public void beforeTest() throws IOException, RestException {

        if (restClient == null) {
            List<InetSocketAddress> addresses = Lists.newArrayList();
            for (HttpServerTransport httpServerTransport : cluster().getInstances(HttpServerTransport.class)) {
                addresses.add(((InetSocketTransportAddress) httpServerTransport.boundAddress().publishAddress()).address());
            }

            String[] specPaths = PathUtils.resolvePathsProperty(REST_TESTS_SPEC, DEFAULT_SPEC_PATH);
            RestSpec restSpec = RestSpec.parseFrom(DEFAULT_SPEC_PATH, specPaths);
            restClient = new RestClient(addresses.toArray(new InetSocketAddress[addresses.size()]), restSpec);
        }
    }
}
