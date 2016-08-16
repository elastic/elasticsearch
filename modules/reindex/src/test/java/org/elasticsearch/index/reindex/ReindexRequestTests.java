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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.reindex.remote.RemoteInfo;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * Tests some of the validation of {@linkplain ReindexRequest}. See reindex's rest tests for much more.
 */
public class ReindexRequestTests extends ESTestCase {
    public void testTimestampAndTtlNotAllowed() {
        ReindexRequest reindex = request();
        reindex.getDestination().ttl("1s").timestamp("now");
        ActionRequestValidationException e = reindex.validate();
        assertEquals("Validation Failed: 1: setting ttl on destination isn't supported. use scripts instead.;"
                + "2: setting timestamp on destination isn't supported. use scripts instead.;",
                e.getMessage());
    }

    public void testReindexFromRemoteDoesNotSupportSearchQuery() {
        ReindexRequest reindex = request();
        reindex.setRemoteInfo(new RemoteInfo(randomAsciiOfLength(5), randomAsciiOfLength(5), between(1, Integer.MAX_VALUE),
                new BytesArray("real_query"), null, null, emptyMap()));
        reindex.getSearchRequest().source().query(matchAllQuery()); // Unsupported place to put query
        ActionRequestValidationException e = reindex.validate();
        assertEquals("Validation Failed: 1: reindex from remote sources should use RemoteInfo's query instead of source's query;",
                e.getMessage());
    }

    private ReindexRequest request() {
        ReindexRequest reindex = new ReindexRequest(new SearchRequest(), new IndexRequest());
        reindex.getSearchRequest().indices("source");
        reindex.getDestination().index("dest");
        return reindex;
    }
}
