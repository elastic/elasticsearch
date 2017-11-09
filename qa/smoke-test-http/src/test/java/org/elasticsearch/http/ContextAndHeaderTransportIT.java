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

package org.elasticsearch.http;

import org.apache.http.message.BasicHeader;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = SUITE)
public class ContextAndHeaderTransportIT extends HttpSmokeTestCase {
    private static final List<RequestAndHeaders> requests =  new CopyOnWriteArrayList<>();
    private static final String CUSTOM_HEADER = "SomeCustomHeader";
    private String randomHeaderValue = randomAlphaOfLength(20);
    private String queryIndex = "query-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
    private String lookupIndex = "lookup-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ActionLoggingPlugin.class);
        plugins.add(CustomHeadersPlugin.class);
        return plugins;
    }

    @Before
    public void createIndices() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("location").field("type", "geo_shape").endObject()
            .startObject("name").field("type", "text").endObject()
            .endObject()
            .endObject().endObject().string();

        Settings settings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1) // A single shard will help to keep the tests repeatable.
            .build();
        assertAcked(transportClient().admin().indices().prepareCreate(lookupIndex)
            .setSettings(settings).addMapping("type", mapping, XContentType.JSON));
        assertAcked(transportClient().admin().indices().prepareCreate(queryIndex)
            .setSettings(settings).addMapping("type", mapping, XContentType.JSON));
        ensureGreen(queryIndex, lookupIndex);
        requests.clear();
    }

    @After
    public void checkAllRequestsContainHeaders() {
        assertRequestsContainHeader(IndexRequest.class);
        assertRequestsContainHeader(RefreshRequest.class);
    }

    public void testThatTermsLookupGetRequestContainsContextAndHeaders() throws Exception {
        transportClient().prepareIndex(lookupIndex, "type", "1")
            .setSource(jsonBuilder().startObject().array("followers", "foo", "bar", "baz").endObject()).get();
        transportClient().prepareIndex(queryIndex, "type", "1")
            .setSource(jsonBuilder().startObject().field("username", "foo").endObject()).get();
        transportClient().admin().indices().prepareRefresh(queryIndex, lookupIndex).get();

        TermsLookup termsLookup = new TermsLookup(lookupIndex, "type", "1", "followers");
        TermsQueryBuilder termsLookupFilterBuilder = QueryBuilders.termsLookupQuery("username", termsLookup);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).must(termsLookupFilterBuilder);

        SearchResponse searchResponse = transportClient()
            .prepareSearch(queryIndex)
            .setQuery(queryBuilder)
            .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);

        assertGetRequestsContainHeaders();
    }



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

        MoreLikeThisQueryBuilder moreLikeThisQueryBuilder = QueryBuilders.moreLikeThisQuery(new String[]{"name"}, null,
            new Item[]{new Item(lookupIndex, "type", "1")})
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

    public void testThatRelevantHttpHeadersBecomeRequestHeaders() throws IOException {
        final String IRRELEVANT_HEADER = "SomeIrrelevantHeader";
        Response response = getRestClient().performRequest("GET", "/" + queryIndex + "/_search",
                new BasicHeader(CUSTOM_HEADER, randomHeaderValue), new BasicHeader(IRRELEVANT_HEADER, randomHeaderValue));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        List<RequestAndHeaders> searchRequests = getRequests(SearchRequest.class);
        assertThat(searchRequests, hasSize(greaterThan(0)));
        for (RequestAndHeaders requestAndHeaders : searchRequests) {
            assertThat(requestAndHeaders.headers.containsKey(CUSTOM_HEADER), is(true));
            // was not specified, thus is not included
            assertThat(requestAndHeaders.headers.containsKey(IRRELEVANT_HEADER), is(false));
        }
    }

    private  List<RequestAndHeaders> getRequests(Class<?> clazz) {
        List<RequestAndHeaders> results = new ArrayList<>();
        for (RequestAndHeaders request : requests) {
            if (request.request.getClass().equals(clazz)) {
                results.add(request);
            }
        }

        return results;
    }

    private void assertRequestsContainHeader(Class<? extends ActionRequest> clazz) {
        List<RequestAndHeaders> classRequests = getRequests(clazz);
        for (RequestAndHeaders request : classRequests) {
            assertRequestContainsHeader(request.request, request.headers);
        }
    }

    private void assertGetRequestsContainHeaders() {
        assertGetRequestsContainHeaders(this.lookupIndex);
    }

    private void assertGetRequestsContainHeaders(String index) {
        List<RequestAndHeaders> getRequests = getRequests(GetRequest.class);
        assertThat(getRequests, hasSize(greaterThan(0)));

        for (RequestAndHeaders request : getRequests) {
            if (!((GetRequest)request.request).index().equals(index)) {
                continue;
            }
            assertRequestContainsHeader(request.request, request.headers);
        }
    }

    private void assertRequestContainsHeader(ActionRequest request, Map<String, String> context) {
        String msg = String.format(Locale.ROOT, "Expected header %s to be in request %s", CUSTOM_HEADER, request.getClass().getName());
        if (request instanceof IndexRequest) {
            IndexRequest indexRequest = (IndexRequest) request;
            msg = String.format(Locale.ROOT, "Expected header %s to be in index request %s/%s/%s", CUSTOM_HEADER,
                indexRequest.index(), indexRequest.type(), indexRequest.id());
        }
        assertThat(msg, context.containsKey(CUSTOM_HEADER), is(true));
        assertThat(context.get(CUSTOM_HEADER).toString(), is(randomHeaderValue));
    }

    /**
     * a transport client that adds our random header
     */
    private Client transportClient() {
        return internalCluster().transportClient().filterWithHeader(Collections.singletonMap(CUSTOM_HEADER, randomHeaderValue));
    }

    public static class ActionLoggingPlugin extends Plugin implements ActionPlugin {

        private final SetOnce<LoggingFilter> loggingFilter = new SetOnce<>();

        @Override
        public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                   ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                   NamedXContentRegistry xContentRegistry, Environment environment,
                                                   NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
            loggingFilter.set(new LoggingFilter(clusterService.getSettings(), threadPool));
            return Collections.emptyList();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(loggingFilter.get());
        }

    }

    public static class LoggingFilter extends ActionFilter.Simple {

        private final ThreadPool threadPool;

        public LoggingFilter(Settings settings, ThreadPool pool) {
            super(settings);
            this.threadPool = pool;
        }

        @Override
        public int order() {
            return 999;
        }

        @Override
        protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
            requests.add(new RequestAndHeaders(threadPool.getThreadContext().getHeaders(), request));
            return true;
        }
    }

    private static class RequestAndHeaders {
        final Map<String, String> headers;
        final ActionRequest request;

        private RequestAndHeaders(Map<String, String> headers, ActionRequest request) {
            this.headers = headers;
            this.request = request;
        }
    }

    public static class CustomHeadersPlugin extends Plugin implements ActionPlugin {
        public Collection<String> getRestHeaders() {
            return Collections.singleton(CUSTOM_HEADER);
        }
    }
}
