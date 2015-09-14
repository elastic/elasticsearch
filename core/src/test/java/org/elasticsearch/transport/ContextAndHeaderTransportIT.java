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

package org.elasticsearch.transport;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsLookupQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.Node.HTTP_ENABLED;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.search.suggest.SuggestBuilders.phraseSuggestion;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSuggestionSize;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasStatus;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = SUITE)
public class ContextAndHeaderTransportIT extends ESIntegTestCase {

    private static final List<ActionRequest> requests =  new CopyOnWriteArrayList<>();
    private String randomHeaderKey = randomAsciiOfLength(10);
    private String randomHeaderValue = randomAsciiOfLength(20);
    private String queryIndex = "query-" + randomAsciiOfLength(10).toLowerCase(Locale.ROOT);
    private String lookupIndex = "lookup-" + randomAsciiOfLength(10).toLowerCase(Locale.ROOT);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("script.indexed", "on")
                .put(HTTP_ENABLED, true)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(ActionLoggingPlugin.class);
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

        requests.clear();
    }

    @After
    public void checkAllRequestsContainHeaders() {
        assertRequestsContainHeader(IndexRequest.class);
        assertRequestsContainHeader(RefreshRequest.class);
    }

    @Test
    public void testThatTermsLookupGetRequestContainsContextAndHeaders() throws Exception {
        transportClient().prepareIndex(lookupIndex, "type", "1")
                .setSource(jsonBuilder().startObject().array("followers", "foo", "bar", "baz").endObject()).get();
        transportClient().prepareIndex(queryIndex, "type", "1")
                .setSource(jsonBuilder().startObject().field("username", "foo").endObject()).get();
        transportClient().admin().indices().prepareRefresh(queryIndex, lookupIndex).get();

        TermsLookupQueryBuilder termsLookupFilterBuilder = QueryBuilders.termsLookupQuery("username").lookupIndex(lookupIndex).lookupType("type").lookupId("1").lookupPath("followers");
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).must(termsLookupFilterBuilder);

        SearchResponse searchResponse = transportClient()
                .prepareSearch(queryIndex)
                .setQuery(queryBuilder)
                .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);

        assertGetRequestsContainHeaders();
    }

    @Test
    public void testThatGeoShapeQueryGetRequestContainsContextAndHeaders() throws Exception {
        transportClient().prepareIndex(lookupIndex, "type", "1").setSource(jsonBuilder().startObject()
                .field("name", "Munich Suburban Area")
                .startObject("location")
                .field("type", "polygon")
                .startArray("coordinates").startArray()
                .startArray().value(11.34).value(48.25).endArray()
                .startArray().value(11.68).value(48.25).endArray()
                .startArray().value(11.65).value(48.06).endArray()
                .startArray().value(11.37).value(48.13).endArray()
                .startArray().value(11.34).value(48.25).endArray() // close the polygon
                .endArray().endArray()
                .endObject()
                .endObject())
                .get();
        // second document
        transportClient().prepareIndex(queryIndex, "type", "1").setSource(jsonBuilder().startObject()
                .field("name", "Munich Center")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(11.57).value(48.13).endArray()
                .endObject()
                .endObject())
                .get();
        transportClient().admin().indices().prepareRefresh(lookupIndex, queryIndex).get();

        GeoShapeQueryBuilder queryBuilder = QueryBuilders.geoShapeQuery("location", "1", "type")
                .indexedShapeIndex(lookupIndex)
                .indexedShapePath("location");

        SearchResponse searchResponse = transportClient()
                .prepareSearch(queryIndex)
                .setQuery(queryBuilder)
                .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(requests, hasSize(greaterThan(0)));

        assertGetRequestsContainHeaders();
    }

    @Test
    public void testThatMoreLikeThisQueryMultiTermVectorRequestContainsContextAndHeaders() throws Exception {
        transportClient().prepareIndex(lookupIndex, "type", "1")
                .setSource(jsonBuilder().startObject().field("name", "Star Wars - The new republic").endObject())
                .get();
        transportClient().prepareIndex(queryIndex, "type", "1")
                .setSource(jsonBuilder().startObject().field("name", "Jar Jar Binks - A horrible mistake").endObject())
                .get();
        transportClient().prepareIndex(queryIndex, "type", "2")
                .setSource(jsonBuilder().startObject().field("name", "Star Wars - Return of the jedi").endObject())
                .get();
        transportClient().admin().indices().prepareRefresh(lookupIndex, queryIndex).get();

        MoreLikeThisQueryBuilder moreLikeThisQueryBuilder = QueryBuilders.moreLikeThisQuery("name")
                .addLikeItem(new Item(lookupIndex, "type", "1"))
                .minTermFreq(1)
                .minDocFreq(1);

        SearchResponse searchResponse = transportClient()
                .prepareSearch(queryIndex)
                .setQuery(moreLikeThisQueryBuilder)
                .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);

        assertRequestsContainHeader(MultiTermVectorsRequest.class);
    }

    @Test
    public void testThatPercolatingExistingDocumentGetRequestContainsContextAndHeaders() throws Exception {
        transportClient().prepareIndex(lookupIndex, ".percolator", "1")
                .setSource(jsonBuilder().startObject().startObject("query").startObject("match").field("name", "star wars").endObject().endObject().endObject())
                .get();
        transportClient().prepareIndex(lookupIndex, "type", "1")
                .setSource(jsonBuilder().startObject().field("name", "Star Wars - The new republic").endObject())
                .get();
        transportClient().admin().indices().prepareRefresh(lookupIndex).get();

        GetRequest getRequest = transportClient().prepareGet(lookupIndex, "type", "1").request();
        PercolateResponse response = transportClient().preparePercolate().setDocumentType("type").setGetRequest(getRequest).get();
        assertThat(response.getCount(), is(1l));

        assertGetRequestsContainHeaders();
    }

    @Test
    public void testThatIndexedScriptGetRequestContainsContextAndHeaders() throws Exception {
        PutIndexedScriptResponse scriptResponse = transportClient().preparePutIndexedScript(GroovyScriptEngineService.NAME, "my_script",
                jsonBuilder().startObject().field("script", "_score * 10").endObject().string()
        ).get();
        assertThat(scriptResponse.isCreated(), is(true));

        transportClient().prepareIndex(queryIndex, "type", "1")
                .setSource(jsonBuilder().startObject().field("name", "Star Wars - The new republic").endObject())
                .get();
        transportClient().admin().indices().prepareRefresh(queryIndex).get();

        // custom content, not sure how to specify "script_id" otherwise in the API
        XContentBuilder builder = jsonBuilder().startObject().startObject("function_score").field("boost_mode", "replace").startArray("functions")
                .startObject().startObject("script_score").field("script_id", "my_script").field("lang", "groovy").endObject().endObject().endArray().endObject().endObject();

        SearchResponse searchResponse = transportClient()
                .prepareSearch(queryIndex)
                .setQuery(builder)
                .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getMaxScore(), is(10.0f));

        assertGetRequestsContainHeaders(".scripts");
        assertRequestsContainHeader(PutIndexedScriptRequest.class);
    }

    @Test
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

    @Test
    public void testThatIndexedScriptGetRequestInReducePhaseContainsContextAndHeaders() throws Exception {
        PutIndexedScriptResponse scriptResponse = transportClient().preparePutIndexedScript(GroovyScriptEngineService.NAME, "my_script",
                jsonBuilder().startObject().field("script", "_value0 * 10").endObject().string()).get();
        assertThat(scriptResponse.isCreated(), is(true));

        transportClient().prepareIndex(queryIndex, "type", "1")
                .setSource(jsonBuilder().startObject().field("s_field", "foo").field("l_field", 10).endObject()).get();
        transportClient().admin().indices().prepareRefresh(queryIndex).get();

        SearchResponse searchResponse = transportClient()
                .prepareSearch(queryIndex)
                .addAggregation(
                        AggregationBuilders
                                .terms("terms")
                                .field("s_field")
                                .subAggregation(AggregationBuilders.max("max").field("l_field"))
                                .subAggregation(
                                        PipelineAggregatorBuilders.bucketScript("scripted").setBucketsPaths("max").script(
                                                new Script("my_script", ScriptType.INDEXED, GroovyScriptEngineService.NAME, null)))).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);

        assertGetRequestsContainHeaders(".scripts");
        assertRequestsContainHeader(PutIndexedScriptRequest.class);
    }

    @Test
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

    @Test
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

        List<IndexRequestBuilder> builders = new ArrayList<>();
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
        String suggestText = "united states house of representatives elections in washington 2006";
        if (suggestText != null) {
            searchRequestBuilder.setSuggestText(suggestText);
        }
        searchRequestBuilder.addSuggestion(filteredFilterSuggest);
        SearchResponse actionGet = searchRequestBuilder.execute().actionGet();
        assertThat(Arrays.toString(actionGet.getShardFailures()), actionGet.getFailedShards(), equalTo(0));
        Suggest searchSuggest = actionGet.getSuggest();

        assertSuggestionSize(searchSuggest, 0, 2, "title");

        assertGetRequestsContainHeaders(".scripts");
        assertRequestsContainHeader(PutIndexedScriptRequest.class);
    }


    @Test
    public void testThatRelevantHttpHeadersBecomeRequestHeaders() throws Exception {
        String releventHeaderName = "relevant_" + randomHeaderKey;
        for (RestController restController : internalCluster().getDataNodeInstances(RestController.class)) {
            restController.registerRelevantHeaders(releventHeaderName);
        }

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpResponse response = new HttpRequestBuilder(httpClient)
                .httpTransport(internalCluster().getDataNodeInstance(HttpServerTransport.class))
                .addHeader(randomHeaderKey, randomHeaderValue)
                .addHeader(releventHeaderName, randomHeaderValue)
                .path("/" + queryIndex + "/_search")
                .execute();

        assertThat(response, hasStatus(OK));
        List<SearchRequest> searchRequests = getRequests(SearchRequest.class);
        assertThat(searchRequests, hasSize(greaterThan(0)));
        for (SearchRequest searchRequest : searchRequests) {
            assertThat(searchRequest.hasHeader(releventHeaderName), is(true));
            // was not specified, thus is not included
            assertThat(searchRequest.hasHeader(randomHeaderKey), is(false));
        }
    }

    private <T> List<T> getRequests(Class<T> clazz) {
        List<T> results = new ArrayList<>();
        for (ActionRequest request : requests) {
            if (request.getClass().equals(clazz)) {
                results.add((T) request);
            }
        }

        return results;
    }

    private void assertRequestsContainHeader(Class<? extends ActionRequest> clazz) {
        List<? extends ActionRequest> classRequests = getRequests(clazz);
        for (ActionRequest request : classRequests) {
            assertRequestContainsHeader(request);
        }
    }

    private void assertGetRequestsContainHeaders() {
        assertGetRequestsContainHeaders(this.lookupIndex);
    }

    private void assertGetRequestsContainHeaders(String index) {
        List<GetRequest> getRequests = getRequests(GetRequest.class);
        assertThat(getRequests, hasSize(greaterThan(0)));

        for (GetRequest request : getRequests) {
            if (!request.index().equals(index)) {
                continue;
            }
            assertRequestContainsHeader(request);
        }
    }

    private void assertRequestContainsHeader(ActionRequest request) {
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
            protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
                request.putHeader(randomHeaderKey, randomHeaderValue);
                super.doExecute(action, request, listener);
            }
        };

        return filterClient;
    }

    public static class ActionLoggingPlugin extends Plugin {

        @Override
        public String name() {
            return "test-action-logging";
        }

        @Override
        public String description() {
            return "Test action logging";
        }

        @Override
        public Collection<Module> nodeModules() {
            return Collections.<Module>singletonList(new ActionLoggingModule());
        }

        public void onModule(ActionModule module) {
            module.registerFilter(LoggingFilter.class);
        }
    }

    public static class ActionLoggingModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(LoggingFilter.class).asEagerSingleton();
        }

    }

    public static class LoggingFilter extends ActionFilter.Simple {

        @Inject
        public LoggingFilter(Settings settings) {
            super(settings);
        }

        @Override
        public int order() {
            return 999;
        }

        @Override
        protected boolean apply(String action, ActionRequest request, ActionListener listener) {
            requests.add(request);
            return true;
        }

        @Override
        protected boolean apply(String action, ActionResponse response, ActionListener listener) {
            return true;
        }
    }
}
