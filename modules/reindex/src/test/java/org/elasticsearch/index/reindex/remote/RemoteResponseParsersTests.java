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

package org.elasticsearch.index.reindex.remote;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class RemoteResponseParsersTests extends ESTestCase {

    /**
     * Check that we can parse shard search failures without index information.
     */
    public void testFailureWithoutIndex() throws IOException {
        ShardSearchFailure failure = new ShardSearchFailure(new EsRejectedExecutionException("exhausted"));
        XContentBuilder builder = jsonBuilder();
        failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(builder)) {
            ScrollableHitSource.SearchFailure parsed = RemoteResponseParsers.SEARCH_FAILURE_PARSER.parse(parser, XContentType.JSON);
            assertNotNull(parsed.getReason());
            assertThat(parsed.getReason().getMessage(), Matchers.containsString("exhausted"));
            assertThat(parsed.getReason(), Matchers.instanceOf(EsRejectedExecutionException.class));
        }
    }
}
