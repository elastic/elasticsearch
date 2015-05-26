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

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableList;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.query.*;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.Node.HTTP_ENABLED;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = SUITE)
public class ContextAndHeaderTransportTests extends ElasticsearchIntegrationTest {

    private static final List<ActionRequest> requests = Collections.synchronizedList(new ArrayList<ActionRequest>());
    private String randomHeaderKey = randomAsciiOfLength(10);
    private String randomHeaderValue = randomAsciiOfLength(20);
    private String queryIndex = "query-" + randomAsciiOfLength(10).toLowerCase(Locale.ROOT);
    private String lookupIndex = "lookup-" + randomAsciiOfLength(10).toLowerCase(Locale.ROOT);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", ActionLoggingPlugin.class.getName())
                .put("script.indexed", "on")
                .put(HTTP_ENABLED, true)
                .build();
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
                .addItem(new MoreLikeThisQueryBuilder.Item(lookupIndex, "type", "1"))
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

        SearchResponse searchResponse = transportClient().prepareSearch(queryIndex)
                .setTemplateName("the_template")
                .setTemplateParams(params)
                .setTemplateType(ScriptService.ScriptType.INDEXED)
                .get();

        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);

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

    public static class ActionLoggingPlugin extends AbstractPlugin {

        @Override
        public String name() {
            return "test-action-logging";
        }

        @Override
        public String description() {
            return "Test action logging";
        }

        @Override
        public Collection<Class<? extends Module>> modules() {
            return ImmutableList.of(ActionLoggingModule.class);
        }
    }

    public static class ActionLoggingModule extends AbstractModule implements PreProcessModule {


        @Override
        protected void configure() {
            bind(LoggingFilter.class).asEagerSingleton();
        }

        @Override
        public void processModule(Module module) {
            if (module instanceof ActionModule) {
                ((ActionModule)module).registerFilter(LoggingFilter.class);
            }
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
