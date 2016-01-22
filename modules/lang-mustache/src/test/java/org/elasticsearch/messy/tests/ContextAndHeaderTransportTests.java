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

package org.elasticsearch.messy.tests;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.test.ActionRecordingPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.suggest.SuggestBuilders.phraseSuggestion;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSuggestionSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = SUITE)
public class ContextAndHeaderTransportTests extends ESIntegTestCase {
    private String randomHeaderKey = randomAsciiOfLength(10);
    private String randomHeaderValue = randomAsciiOfLength(20);
    private String queryIndex = "query-" + randomAsciiOfLength(10).toLowerCase(Locale.ROOT);
    private String lookupIndex = "lookup-" + randomAsciiOfLength(10).toLowerCase(Locale.ROOT);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("script.indexed", "on")
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(ActionRecordingPlugin.class, MustachePlugin.class);
    }

    @Before
    public void createIndices() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("location").field("type", "geo_shape").endObject()
                .startObject("name").field("type", "string").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings settings = settingsBuilder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, 1) // A single shard will help to keep the tests repeatable.
                .build();
        assertAcked(transportClient().admin().indices().prepareCreate(lookupIndex)
                .setSettings(settings).addMapping("type", mapping));
        assertAcked(transportClient().admin().indices().prepareCreate(queryIndex)
                .setSettings(settings).addMapping("type", mapping));
        ensureGreen(queryIndex, lookupIndex);
    }

    @After
    public void checkAllRequestsContainHeaders() {
        assertRequestsContainHeader(IndexRequest.class);
        assertRequestsContainHeader(RefreshRequest.class);
        ActionRecordingPlugin.clear();
    }

    public void testThatIndexedScriptGetRequestInTemplateQueryContainsContextAndHeaders() throws Exception {
        PutIndexedScriptResponse scriptResponse = transportClient()
                .preparePutIndexedScript(
                        MustacheScriptEngineService.NAME,
                        "my_script",
                        jsonBuilder().startObject().field("script", "{ \"match\": { \"name\": \"Star Wars\" }}").endObject()
                                .string()).get();
        assertThat(scriptResponse.isCreated(), is(true));

        transportClient().prepareIndex(queryIndex, "type", "1")
                .setSource(jsonBuilder().startObject().field("name", "Star Wars - The new republic").endObject()).get();
        transportClient().admin().indices().prepareRefresh(queryIndex).get();

        SearchResponse searchResponse = transportClient()
                .prepareSearch(queryIndex)
                .setQuery(
                        QueryBuilders.templateQuery(new Template("my_script", ScriptType.INDEXED,
                                MustacheScriptEngineService.NAME, null, null))).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);

        assertGetRequestsContainHeaders(".scripts");
        assertRequestsContainHeader(PutIndexedScriptRequest.class);
    }

    public void testThatSearchTemplatesWithIndexedTemplatesGetRequestContainsContextAndHeaders() throws Exception {
        PutIndexedScriptResponse scriptResponse = transportClient().preparePutIndexedScript(MustacheScriptEngineService.NAME, "the_template",
                jsonBuilder().startObject().startObject("template").startObject("query").startObject("match")
                        .field("name", "{{query_string}}").endObject().endObject().endObject().endObject().string()
        ).get();
        assertThat(scriptResponse.isCreated(), is(true));

        transportClient().prepareIndex(queryIndex, "type", "1")
                .setSource(jsonBuilder().startObject().field("name", "Star Wars - The new republic").endObject())
                .get();
        transportClient().admin().indices().prepareRefresh(queryIndex).get();

        Map<String, Object> params = new HashMap<>();
        params.put("query_string", "star wars");

        SearchResponse searchResponse = transportClient().prepareSearch(queryIndex).setTemplate(new Template("the_template", ScriptType.INDEXED, MustacheScriptEngineService.NAME, null, params))
                .get();

        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);

        assertGetRequestsContainHeaders(".scripts");
        assertRequestsContainHeader(PutIndexedScriptRequest.class);
    }

    public void testThatIndexedScriptGetRequestInPhraseSuggestContainsContextAndHeaders() throws Exception {
        CreateIndexRequestBuilder builder = transportClient().admin().indices().prepareCreate("test").setSettings(settingsBuilder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, 1) // A single shard will help to keep the tests repeatable.
                .put("index.analysis.analyzer.text.tokenizer", "standard")
                .putArray("index.analysis.analyzer.text.filter", "lowercase", "my_shingle")
                .put("index.analysis.filter.my_shingle.type", "shingle")
                .put("index.analysis.filter.my_shingle.output_unigrams", true)
                .put("index.analysis.filter.my_shingle.min_shingle_size", 2)
                .put("index.analysis.filter.my_shingle.max_shingle_size", 3));

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("title")
                .field("type", "string")
                .field("analyzer", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        List<String> titles = new ArrayList<>();

        titles.add("United States House of Representatives Elections in Washington 2006");
        titles.add("United States House of Representatives Elections in Washington 2005");
        titles.add("State");
        titles.add("Houses of Parliament");
        titles.add("Representative Government");
        titles.add("Election");

        for (String title: titles) {
            transportClient().prepareIndex("test", "type1").setSource("title", title).get();
        }
        transportClient().admin().indices().prepareRefresh("test").get();

        String filterStringAsFilter = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("match_phrase")
                .field("title", "{{suggestion}}")
                .endObject()
                .endObject()
                .string();

        PutIndexedScriptResponse scriptResponse = transportClient()
                .preparePutIndexedScript(
                        MustacheScriptEngineService.NAME,
                        "my_script",
                jsonBuilder().startObject().field("script", filterStringAsFilter).endObject()
                                .string()).get();
        assertThat(scriptResponse.isCreated(), is(true));

        PhraseSuggestionBuilder suggest = phraseSuggestion("title")
                .field("title")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("title")
                        .suggestMode("always")
                        .maxTermFreq(.99f)
                        .size(10)
                        .maxInspections(200)
                )
                .confidence(0f)
                .maxErrors(2f)
                .shardSize(30000)
                .size(10);

        PhraseSuggestionBuilder filteredFilterSuggest = suggest.collateQuery(new Template("my_script", ScriptType.INDEXED,
                MustacheScriptEngineService.NAME, null, null));

        SearchRequestBuilder searchRequestBuilder = transportClient().prepareSearch("test").setSize(0);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        String suggestText = "united states house of representatives elections in washington 2006";
        if (suggestText != null) {
            suggestBuilder.setText(suggestText);
        }
        suggestBuilder.addSuggestion(filteredFilterSuggest);
        searchRequestBuilder.suggest(suggestBuilder);
        SearchResponse actionGet = searchRequestBuilder.execute().actionGet();
        assertThat(Arrays.toString(actionGet.getShardFailures()), actionGet.getFailedShards(), equalTo(0));
        Suggest searchSuggest = actionGet.getSuggest();

        assertSuggestionSize(searchSuggest, 0, 2, "title");

        assertGetRequestsContainHeaders(".scripts");
        assertRequestsContainHeader(PutIndexedScriptRequest.class);
    }

    private void assertRequestsContainHeader(Class<? extends ActionRequest<?>> clazz) {
        List<? extends ActionRequest<?>> classRequests = ActionRecordingPlugin.requestsOfType(clazz);
        for (ActionRequest<?> request : classRequests) {
            assertRequestContainsHeader(request);
        }
    }

    private void assertGetRequestsContainHeaders(String index) {
        List<GetRequest> getRequests = ActionRecordingPlugin.requestsOfType(GetRequest.class);
        assertThat(getRequests, hasSize(greaterThan(0)));

        for (GetRequest request : getRequests) {
            if (!request.index().equals(index)) {
                continue;
            }
            assertRequestContainsHeader(request);
        }
    }

    private void assertRequestContainsHeader(ActionRequest<?> request) {
        String msg = String.format(Locale.ROOT, "Expected header %s to be in request %s", randomHeaderKey, request.getClass().getName());
        if (request instanceof IndexRequest) {
            IndexRequest indexRequest = (IndexRequest) request;
            msg = String.format(Locale.ROOT, "Expected header %s to be in index request %s/%s/%s", randomHeaderKey,
                    indexRequest.index(), indexRequest.type(), indexRequest.id());
        }
        assertThat(msg, request.hasHeader(randomHeaderKey), is(true));
        assertThat(request.getHeader(randomHeaderKey).toString(), is(randomHeaderValue));
    }

    /**
     * a transport client that adds our random header
     */
    private Client transportClient() {
        Client transportClient = internalCluster().transportClient();
        FilterClient filterClient = new FilterClient(transportClient) {
            @Override
            protected <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
                    Action<Request, Response, RequestBuilder> action, Request request,
                    ActionListener<Response> listener) {
                request.putHeader(randomHeaderKey, randomHeaderValue);
                super.doExecute(action, request, listener);
            }
        };

        return filterClient;
    }
}
