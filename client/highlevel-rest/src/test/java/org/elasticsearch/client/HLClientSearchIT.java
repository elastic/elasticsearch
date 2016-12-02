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

import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.highlevel.ESHighLevelTestCase;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;

public class HLClientSearchIT extends ESHighLevelTestCase {

    public void createTestDoc() throws IOException {
        XContentBuilder mappingsBuilder = randomXContentBuilder();
        mappingsBuilder.startObject();
        mappingsBuilder.startObject("mappings");
        mappingsBuilder.startObject("type");
        mappingsBuilder.startObject("properties");
        mappingsBuilder.startObject("title");
        mappingsBuilder.field("type", "text");
        mappingsBuilder.field("store", "true");
        mappingsBuilder.endObject();
        mappingsBuilder.startObject("content");
        mappingsBuilder.field("type", "text");
        mappingsBuilder.field("store", "true");
        mappingsBuilder.endObject();
        mappingsBuilder.endObject();
        mappingsBuilder.endObject();
        mappingsBuilder.endObject();
        mappingsBuilder.endObject();

        Map<String, String> params = new HashMap<>();
        client().performRequest("PUT", "test", params,
                new StringEntity(mappingsBuilder.string()));
        params.put("refresh", "wait_for");

        XContentBuilder document = randomXContentBuilder();
        document.startObject();
        document.startArray("content");
        document.value("buzz cola");
        document.value("some buzz");
        document.endArray();
        document.field("title", "some title");
        document.endObject();
        client().performRequest("PUT", "test/type/1", params, new StringEntity(document.string()));
    }

    public void testSearch() throws IOException {
        createTestDoc();
        SearchResponse searchResponse = aClient.performSearchRequest(new SearchRequest(
                new SearchSourceBuilder()
                .query(new MatchQueryBuilder("content", "buzz").queryName("buzz_query"))
                .version(true)
                .storedFields(Arrays.asList("_source", "content", "title"))
                .highlighter(new HighlightBuilder().field("content"))
                .sort(new ScoreSortBuilder().order(SortOrder.ASC))
                .trackScores(true)));
        assertFalse(searchResponse.isTimedOut());
        assertTrue(searchResponse.getTookInMillis() > 0);
        assertEquals(5, searchResponse.getTotalShards());
        assertEquals(5, searchResponse.getSuccessfulShards());
        assertEquals(0, searchResponse.getFailedShards());
        SearchHits hits = searchResponse.getHits();
        assertEquals(1, hits.getTotalHits());
        assertEquals(1, hits.totalHits());
        assertThat(hits.maxScore(), greaterThan(0.0f));
        assertThat(hits.getMaxScore(), greaterThan(0.0f));
        SearchHit searchHit = hits.hits()[0];
        assertEquals("some title", searchHit.sourceAsMap().get("title"));
        assertEquals("test", searchHit.index());
        assertEquals("type", searchHit.type());
        assertEquals("1", searchHit.id());
        assertEquals(1, searchHit.version());
        float score = searchHit.score();
        assertThat(score, greaterThan(0.0f));
        assertEquals(2, searchHit.fields().size());
        assertThat(searchHit.field("content").getValues(), contains("buzz cola", "some buzz"));
        assertEquals("some title", searchHit.field("title").getValue());
        assertNull(searchHit.field("something"));
        assertEquals(1, searchHit.highlightFields().size());
        assertEquals("content", searchHit.highlightFields().get("content").name());
        assertThat(Arrays.asList(searchHit.highlightFields().get("content").fragments()),
                contains(new Text("<em>buzz</em> cola"), new Text("some <em>buzz</em>")));
        assertEquals(1, searchHit.sortValues().length);
        assertEquals(score, ((Double) searchHit.sortValues()[0]).floatValue(), Float.MIN_VALUE);
        assertEquals(1, searchHit.matchedQueries().length);
        assertEquals("buzz_query", searchHit.matchedQueries()[0]);
    }

    private static XContentBuilder randomXContentBuilder() throws IOException {
        //only string based formats are supported, no cbor nor smile
        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.YAML);
        return XContentBuilder.builder(XContentFactory.xContent(xContentType));
    }

}
