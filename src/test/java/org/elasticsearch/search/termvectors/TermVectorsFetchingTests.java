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

package org.elasticsearch.search.termvectors;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.fetch.termvectors.TermVectorsBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.hamcrest.Matchers.*;

public class TermVectorsFetchingTests extends ElasticsearchIntegrationTest {

    @Test
    public void testTermVectorsSimple() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type", 
                        "with_tvs", "type=string,term_vector=yes,analyzer=whitespace", 
                        "without_tvs", "type=string,analyzer=whitespace"));
        ensureGreen();

        String text = "the quick brown fox jumps over the lazy dog";
        client().prepareIndex("test", "type", "1").setSource("with_tvs", text, "without_tvs", text).get();
        refresh();

        // fetch stored term vectors
        SearchResponse response = client().prepareSearch("test").setTermVectors(true).get();
        assertThat(response.getHits().getAt(0).getTermVectorsResponse(), notNullValue());
        assertThat(response.getHits().getAt(0).getTermVectorsResponse().getFields().size(), Matchers.equalTo(1));
        assertThat(response.getHits().getAt(0).getTermVectorsResponse().getFields().terms("with_tvs").size(), Matchers.equalTo(8l));
        
        // do not fetch term vectors
        response = client().prepareSearch("test").get();
        assertThat(response.getHits().getAt(0).getTermVectorsResponse(), nullValue());
        response = client().prepareSearch("test").setTermVectors(false).get();
        assertThat(response.getHits().getAt(0).getTermVectorsResponse(), nullValue());

        // fetch stored term vectors and generate the ones not stored
        response = client().prepareSearch("test").setTermVectors(new TermVectorsBuilder().setSelectedFields("*")).get();
        assertThat(response.getHits().getAt(0).getTermVectorsResponse(), notNullValue());
        assertThat(response.getHits().getAt(0).getTermVectorsResponse().getFields().size(), Matchers.equalTo(2));
        assertThat(response.getHits().getAt(0).getTermVectorsResponse().getFields().terms("with_tvs").size(), Matchers.equalTo(8l));
        assertThat(response.getHits().getAt(0).getTermVectorsResponse().getFields().terms("without_tvs").size(), Matchers.equalTo(8l));
    }

    @Test
    public void testTermVectorsDisallowedParameters() throws IOException {
        createIndex("test");
        ensureGreen();

        String[] disallowedParameters = new String[]{"_index", "_type", "_id", "doc", "_routing", "routing",
                "_version", "version", "_version_type", "version_type", "_versionType", "versionType"};
        for (String param : disallowedParameters) {
            String request = 
                    "{\"" +
                        "query\":" +
                            "{\"match_all\":{}}," +
                        "\"term_vectors\": " + "{\"" + param + "\":\"\"}" +
                    "}";
            try {
                client().prepareSearch("test").setTypes("type").setSource(new BytesArray(new BytesRef(request))).get();
                fail("Expected search phase execution failure.");
            } catch (SearchPhaseExecutionException e) {
                assertTrue(e.getMessage().contains("The parameter \"" + param + "\" is not allowed!"));
            }
        }
    }    
}
