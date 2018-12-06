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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.util.regex.Matcher;

import static org.elasticsearch.common.logging.DeprecationLogger.WARNING_HEADER_PATTERN;
import static org.hamcrest.Matchers.equalTo;

public class DefaultShardsIT extends ESRestTestCase {

    public void testDefaultShards() throws IOException {
        final Response response = client().performRequest(new Request("PUT", "/index"));
        final String warning = response.getHeader("Warning");
        if (warning == null) {
            StringBuilder explanation = new StringBuilder("expected response to contain a warning but did not ");
            explanation.append(response);
            if (response.getEntity() != null) {
                explanation.append(" entity:\n").append(EntityUtils.toString(response.getEntity()));
            }
            fail(explanation.toString());
        }
        final Matcher matcher = WARNING_HEADER_PATTERN.matcher(warning);
        assertTrue("warning didn't match warning header pattern but was [" + warning + "]", matcher.matches());
        final String message = matcher.group(1);
        assertThat(message, equalTo("the default number of shards will change from [5] to [1] in 7.0.0; "
                + "if you wish to continue using the default of [5] shards, "
                + "you must manage this on the create index request or with an index template"));
    }

    public void testNonDefaultShards() throws IOException {
        final Request request = new Request("PUT", "/index");
        request.setJsonEntity("{\"settings\":{\"index.number_of_shards\":1}}");
        final Response response = client().performRequest(request);
        assertNull(response.getHeader("Warning"));
    }

}
