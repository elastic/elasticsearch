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

package org.elasticsearch.client.security;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ClearRolesCacheResponseTests extends ESTestCase {

    public void testParseFromXContent() throws IOException {
        final ElasticsearchException exception = new ElasticsearchException("test");
        final String nodesHeader = "\"_nodes\": { \"total\": 2, \"successful\": 1, \"failed\": 1, \"failures\": [ "
            + Strings.toString(exception) + "] },";
        final String clusterName = "\"cluster_name\": \"cn\",";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, "{" + nodesHeader + clusterName +  "\"nodes\" : {} }")) {

            ClearRolesCacheResponse response = ClearRolesCacheResponse.fromXContent(parser);
            assertNotNull(response);
            assertThat(response.getNodes(), empty());
            assertThat(response.getClusterName(), equalTo("cn"));
            assertThat(response.getHeader().getSuccessful(), equalTo(1));
            assertThat(response.getHeader().getFailed(), equalTo(1));
            assertThat(response.getHeader().getTotal(), equalTo(2));
            assertThat(response.getHeader().getFailures(), hasSize(1));
            assertThat(response.getHeader().getFailures().get(0).getMessage(), containsString("reason=test"));
        }

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                "{" + nodesHeader + clusterName + "\"nodes\" : { \"id1\": { \"name\": \"a\"}, \"id2\": { \"name\": \"b\"}}}"
            )
        ) {

            ClearRolesCacheResponse response = ClearRolesCacheResponse.fromXContent(parser);
            assertNotNull(response);
            assertThat(response.getNodes(), hasSize(2));
            assertThat(response.getNodes().get(0).getId(), equalTo("id1"));
            assertThat(response.getNodes().get(0).getName(), equalTo("a"));
            assertThat(response.getNodes().get(1).getId(), equalTo("id2"));
            assertThat(response.getNodes().get(1).getName(), equalTo("b"));
        }
    }
}
