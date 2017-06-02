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

import org.apache.http.HttpEntity;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SortBy;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Test usage of a custom suggester provided by a plugin with the {@link RestHighLevelClient}.
 */
public class RestHighLevelClientWithCustomSuggesterTests extends RestHighLevelClientWithPluginTestCase {

    private static final String CUSTOM = "custom";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return singletonList(CustomPlugin.class);
    }

    public void testCustomSuggester() throws Exception {
        final int numSuggestions = randomIntBetween(1, 5);
        final int[] customNumbers = new int[numSuggestions];

        SuggestBuilder suggestBuilder = new SuggestBuilder();
        for (int i = 0; i < numSuggestions; i++) {
            customNumbers[i] = randomIntBetween(0, 10);
            suggestBuilder.addSuggestion("suggest_" + i,
                    new CustomSuggestionBuilder("field", customNumbers[i]).text("custom number is " + customNumbers[i]));
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.suggest(suggestBuilder);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = search(searchRequest);

        Suggest suggest = searchResponse.getSuggest();
        assertEquals(numSuggestions, suggest.size());
        for (int i = 0; i < numSuggestions; i++) {
            TermSuggestion suggestion = suggest.getSuggestion("suggest_" + i);
            assertEquals(customNumbers[i], suggestion.getEntries().size());

            for (int j = 0; j < customNumbers[i]; j++) {
                assertEquals("term " + j, suggestion.getEntries().get(j).getText().string());
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Response performRequest(HttpEntity httpEntity) throws IOException {
        try (XContentParser parser = createParser(Request.REQUEST_BODY_CONTENT_TYPE.xContent(), httpEntity.getContent())) {
            Map<String, ?> requestAsMap = parser.map();
            assertEquals(2, requestAsMap.size());
            assertTrue("Search request does not contain the match all query", requestAsMap.containsKey("query"));
            assertTrue("Search request does not contain any suggest", requestAsMap.containsKey("suggest"));

            Map<String, ?> queryAsMap = (Map<String, ?>) requestAsMap.get("query");
            assertEquals(1, queryAsMap.size());
            assertTrue("Query must be a match query", queryAsMap.containsKey("match_all"));

            Map<String, ?> suggestAsMap = (Map<String, ?>) requestAsMap.get("suggest");
            assertThat(suggestAsMap.size(), greaterThan(0));

            List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions =
                    new ArrayList<>();

            for (Map.Entry<String, ?> suggestEntry : suggestAsMap.entrySet()) {
                assertTrue(suggestEntry.getKey().startsWith("suggest_"));

                Map<String, ?> suggestEntryAsMap = (Map<String, ?>) suggestEntry.getValue();
                assertTrue("Suggest must have 'custom' type", suggestEntryAsMap.containsKey(CUSTOM));
                assertTrue("Suggest must have some text", suggestEntryAsMap.containsKey("text"));

                Map<String, ?> customSuggestAsMap = (Map<String, ?>) suggestEntryAsMap.get(CUSTOM);
                assertTrue("Custom suggest must contain the random number", customSuggestAsMap.containsKey("number"));

                int customNumber = ((Number) customSuggestAsMap.get("number")).intValue();
                assertEquals("custom number is " + customNumber, suggestEntryAsMap.get("text"));

                TermSuggestion suggestion = new TermSuggestion(suggestEntry.getKey(), customNumber, SortBy.SCORE);
                for (int i = 0; i < customNumber; i++) {
                    suggestion.addTerm(new TermSuggestion.Entry(new Text("term " + i), i, customNumber));
                }
                suggestions.add(suggestion);
            }

            Suggest suggests = new Suggest(suggestions);
            SearchResponse searchResponse =
                    new SearchResponse(
                            new SearchResponseSections(SearchHits.empty(), null, suggests, false, false, null, 1),
                            randomAlphaOfLengthBetween(5, 10), 5, 5, 100, ShardSearchFailure.EMPTY_ARRAY);
            return createResponse(searchResponse);
        }
    }

    /**
     * A plugin that provides a custom suggester.
     */
    public static class CustomPlugin extends Plugin implements SearchPlugin {

        public CustomPlugin() {
        }

        @Override
        public List<SuggesterSpec<?>> getSuggesters() {
            return singletonList(new SuggesterSpec<>(CUSTOM, CustomSuggestionBuilder::new, CustomSuggestionBuilder::fromXContent));
        }
    }

    static class CustomSuggestionBuilder extends TermSuggestionBuilder {

        private final int customNumber;

        CustomSuggestionBuilder(String field, int customNumber) {
            super(field);
            this.customNumber = customNumber;
        }

        CustomSuggestionBuilder(StreamInput in) throws IOException {
            super(in);
            this.customNumber = in.readInt();
        }

        @Override
        public String getWriteableName() {
            return CUSTOM;
        }

        @Override
        public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("number", customNumber);
            return super.innerToXContent(builder, params);
        }
    }
}
