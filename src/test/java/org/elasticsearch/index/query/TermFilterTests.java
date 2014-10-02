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

package org.elasticsearch.index.query;

import org.junit.Test;


import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.*;

/**
 */
public class TermFilterTests extends TermQueryTests {

    @Test
    public void testIndexTermFilter() throws InterruptedException, ExecutionException {
        createIndex( "test1", "test2");
        ensureGreen( "test1", "test2");

        long docsInTest1 = scaledRandomIntBetween(10,100);
        long docsInTest2 = scaledRandomIntBetween(10,100);

        indexDocsToIndex("test1", docsInTest1);
        indexDocsToIndex("test2", docsInTest2);

        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("_all");
            String query = "{\"query\":\n" +
                    "{\n" +
                    "  \"filtered\": {\n" +
                    "    \"query\": {\n" +
                    "      \"match_all\": {}\n" +
                    "    },\n" +
                    "    \"filter\": {\n" +
                    "      \"term\": { \"_index\": { \"value\": \"test1\" }}\n" +
                    "    }\n" +
                    "  }\n" +
                    "} }";

            BytesReference bytesRef = new BytesArray(query);
            searchRequest.source(bytesRef, false);
            SearchResponse response = client().search(searchRequest).get();

            assertThat(response.getHits().totalHits(), is(docsInTest1));
        }

        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("_all");

            String query = "{\"query\":\n" +
                    "{\n" +
                    "  \"filtered\": {\n" +
                    "    \"query\": {\n" +
                    "      \"match_all\": {}\n" +
                    "    },\n" +
                    "    \"filter\": {\n" +
                    "      \"term\": { \"_index\": { \"value\": \"test2\" }}\n" +
                    "    }\n" +
                    "  }\n" +
                    "} }";

            BytesReference bytesRef = new BytesArray(query);
            searchRequest.source(bytesRef, false);
            SearchResponse response = client().search(searchRequest).get();

            assertThat(response.getHits().totalHits(), is(docsInTest2));
        }

    }

}
