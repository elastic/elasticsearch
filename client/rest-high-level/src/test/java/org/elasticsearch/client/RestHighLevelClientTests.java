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

import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestHighLevelClientTests extends ESTestCase {

    static final ProtocolVersion HTTP_PROTOCOL = new ProtocolVersion("http", 1, 1);

    private RestClient restClient;
    private RestHighLevelClient restHighLevelClient;

    @Before
    public void initClient() {
        restClient = mock(RestClient.class);
        restHighLevelClient = new RestHighLevelClient(restClient, RestClient::close, Collections.emptyList());
    }

    public void testCloseIsIdempotent() throws IOException {
        restHighLevelClient.close();
        verify(restClient, times(1)).close();
        restHighLevelClient.close();
        verify(restClient, times(2)).close();
        restHighLevelClient.close();
        verify(restClient, times(3)).close();
    }

    public void testPingSuccessful() throws IOException {
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(RestStatus.OK));
        when(restClient.performRequest(any(Request.class))).thenReturn(response);
        assertTrue(restHighLevelClient.ping(RequestOptions.DEFAULT));
    }

    public void testPing404NotFound() throws IOException {
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(RestStatus.NOT_FOUND));
        when(restClient.performRequest(any(Request.class))).thenReturn(response);
        assertFalse(restHighLevelClient.ping(RequestOptions.DEFAULT));
    }

    public void testPingSocketTimeout() throws IOException {
        when(restClient.performRequest(any(Request.class))).thenThrow(new SocketTimeoutException());
        expectThrows(SocketTimeoutException.class, () -> restHighLevelClient.ping(RequestOptions.DEFAULT));
    }

    public void testInfo() throws IOException {
        MainResponse testInfo = new MainResponse("nodeName", Version.CURRENT, new ClusterName("clusterName"), "clusterUuid",
                Build.CURRENT);
        mockResponse(testInfo);
        MainResponse receivedInfo = restHighLevelClient.info(RequestOptions.DEFAULT);
        assertEquals(testInfo, receivedInfo);
    }

    public void testSearchScroll() throws IOException {
        SearchResponse mockSearchResponse = new SearchResponse(new SearchResponseSections(SearchHits.empty(), InternalAggregations.EMPTY,
                null, false, false, null, 1), randomAlphaOfLengthBetween(5, 10), 5, 5, 0, 100, ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);
        mockResponse(mockSearchResponse);
        SearchResponse searchResponse = restHighLevelClient.scroll(
                new SearchScrollRequest(randomAlphaOfLengthBetween(5, 10)), RequestOptions.DEFAULT);
        assertEquals(mockSearchResponse.getScrollId(), searchResponse.getScrollId());
        assertEquals(0, searchResponse.getHits().totalHits);
        assertEquals(5, searchResponse.getTotalShards());
        assertEquals(5, searchResponse.getSuccessfulShards());
        assertEquals(100, searchResponse.getTook().getMillis());
    }

    public void testClearScroll() throws IOException {
        ClearScrollResponse mockClearScrollResponse = new ClearScrollResponse(randomBoolean(), randomIntBetween(0, Integer.MAX_VALUE));
        mockResponse(mockClearScrollResponse);
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(randomAlphaOfLengthBetween(5, 10));
        ClearScrollResponse clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        assertEquals(mockClearScrollResponse.isSucceeded(), clearScrollResponse.isSucceeded());
        assertEquals(mockClearScrollResponse.getNumFreed(), clearScrollResponse.getNumFreed());
    }

    private void mockResponse(ToXContent toXContent) throws IOException {
        Response response = mock(Response.class);
        ContentType contentType = ContentType.parse(RequestConverters.REQUEST_BODY_CONTENT_TYPE.mediaType());
        String requestBody = toXContent(toXContent, RequestConverters.REQUEST_BODY_CONTENT_TYPE, false).utf8ToString();
        when(response.getEntity()).thenReturn(new NStringEntity(requestBody, contentType));
        when(restClient.performRequest(any(Request.class))).thenReturn(response);
    }

    public void testApiNamingConventions() throws Exception {
        //this list should be empty once the high-level client is feature complete
        String[] notYetSupportedApi = new String[]{
            "cluster.remote_info",
            "count",
            "create",
            "exists_source",
            "get_source",
            "indices.delete_alias",
            "indices.delete_template",
            "indices.exists_template",
            "indices.exists_type",
            "indices.get_upgrade",
            "indices.put_alias",
            "mtermvectors",
            "render_search_template",
            "scripts_painless_execute",
            "tasks.get",
            "termvectors"
        };
        //These API are not required for high-level client feature completeness
        String[] notRequiredApi = new String[] {
            "cluster.allocation_explain",
            "cluster.pending_tasks",
            "cluster.reroute",
            "cluster.state",
            "cluster.stats",
            "indices.shard_stores",
            "indices.upgrade",
            "indices.recovery",
            "indices.segments",
            "indices.stats",
            "ingest.processor_grok",
            "nodes.info",
            "nodes.stats",
            "nodes.hot_threads",
            "nodes.usage",
            "nodes.reload_secure_settings",
            "search_shards",
        };
        Set<String> deprecatedMethods = new HashSet<>();
        deprecatedMethods.add("indices.force_merge");
        deprecatedMethods.add("multi_get");
        deprecatedMethods.add("multi_search");
        deprecatedMethods.add("search_scroll");

        ClientYamlSuiteRestSpec restSpec = ClientYamlSuiteRestSpec.load("/rest-api-spec/api");
        Set<String> apiSpec = restSpec.getApis().stream().map(ClientYamlSuiteRestApi::getName).collect(Collectors.toSet());

        Set<String> topLevelMethodsExclusions = new HashSet<>();
        topLevelMethodsExclusions.add("getLowLevelClient");
        topLevelMethodsExclusions.add("close");

        Map<String, Method> methods = Arrays.stream(RestHighLevelClient.class.getMethods())
                .filter(method -> method.getDeclaringClass().equals(RestHighLevelClient.class)
                        && topLevelMethodsExclusions.contains(method.getName()) == false)
                .map(method -> Tuple.tuple(toSnakeCase(method.getName()), method))
                .flatMap(tuple -> tuple.v2().getReturnType().getName().endsWith("Client")
                        ? getSubClientMethods(tuple.v1(), tuple.v2().getReturnType()) : Stream.of(tuple))
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

        Set<String> apiNotFound = new HashSet<>();

        for (Map.Entry<String, Method> entry : methods.entrySet()) {
            Method method = entry.getValue();
            String apiName = entry.getKey();

            assertTrue("method [" + apiName + "] is not final",
                    Modifier.isFinal(method.getClass().getModifiers()) || Modifier.isFinal(method.getModifiers()));
            assertTrue(Modifier.isPublic(method.getModifiers()));

            //we convert all the method names to snake case, hence we need to look for the '_async' suffix rather than 'Async'
            if (apiName.endsWith("_async")) {
                assertTrue("async method [" + method.getName() + "] doesn't have corresponding sync method",
                        methods.containsKey(apiName.substring(0, apiName.length() - 6)));
                assertThat(method.getReturnType(), equalTo(Void.TYPE));
                assertEquals(0, method.getExceptionTypes().length);
                assertEquals(3, method.getParameterTypes().length);
                assertThat(method.getParameterTypes()[0].getSimpleName(), endsWith("Request"));
                assertThat(method.getParameterTypes()[1], equalTo(RequestOptions.class));
                assertThat(method.getParameterTypes()[2], equalTo(ActionListener.class));
            } else {
                //A few methods return a boolean rather than a response object
                if (apiName.equals("ping") || apiName.contains("exist")) {
                    assertThat(method.getReturnType().getSimpleName(), equalTo("boolean"));
                } else {
                    assertThat(method.getReturnType().getSimpleName(), endsWith("Response"));
                }

                assertEquals(1, method.getExceptionTypes().length);
                //a few methods don't accept a request object as argument
                if (apiName.equals("ping") || apiName.equals("info")) {
                    assertEquals(1, method.getParameterTypes().length);
                    assertThat(method.getParameterTypes()[0], equalTo(RequestOptions.class));
                } else {
                    assertEquals(apiName, 2, method.getParameterTypes().length);
                    assertThat(method.getParameterTypes()[0].getSimpleName(), endsWith("Request"));
                    assertThat(method.getParameterTypes()[1], equalTo(RequestOptions.class));
                }

                boolean remove = apiSpec.remove(apiName);
                if (remove == false) {
                    if (deprecatedMethods.contains(apiName)) {
                        assertTrue("method [" + method.getName() + "], api [" + apiName + "] should be deprecated",
                            method.isAnnotationPresent(Deprecated.class));
                    } else {
                        //TODO xpack api are currently ignored, we need to load xpack yaml spec too
                        if (apiName.startsWith("xpack.") == false &&
                            apiName.startsWith("license.") == false &&
                            apiName.startsWith("machine_learning.") == false &&
                            apiName.startsWith("rollup.") == false &&
                            apiName.startsWith("watcher.") == false &&
                            apiName.startsWith("graph.") == false &&
                            apiName.startsWith("migration.") == false &&
                            apiName.startsWith("security.") == false) {
                            apiNotFound.add(apiName);
                        }
                    }
                }
            }
        }
        assertThat("Some client method doesn't match a corresponding API defined in the REST spec: " + apiNotFound,
            apiNotFound.size(), equalTo(0));

        //we decided not to support cat API in the high-level REST client, they are supposed to be used from a low-level client
        apiSpec.removeIf(api -> api.startsWith("cat."));
        Stream.concat(Arrays.stream(notYetSupportedApi), Arrays.stream(notRequiredApi)).forEach(
            api -> assertTrue(api + " API is either not defined in the spec or already supported by the high-level client",
                apiSpec.remove(api)));
        assertThat("Some API are not supported but they should be: " + apiSpec, apiSpec.size(), equalTo(0));
    }

    private static Stream<Tuple<String, Method>> getSubClientMethods(String namespace, Class<?> clientClass) {
        return Arrays.stream(clientClass.getMethods()).filter(method -> method.getDeclaringClass().equals(clientClass))
                .map(method -> Tuple.tuple(namespace + "." + toSnakeCase(method.getName()), method))
                .flatMap(tuple -> tuple.v2().getReturnType().getName().endsWith("Client")
                    ? getSubClientMethods(tuple.v1(), tuple.v2().getReturnType()) : Stream.of(tuple));
    }

    private static String toSnakeCase(String camelCase) {
        StringBuilder snakeCaseString = new StringBuilder();
        for (Character aChar : camelCase.toCharArray()) {
            if (Character.isUpperCase(aChar)) {
                snakeCaseString.append('_');
                snakeCaseString.append(Character.toLowerCase(aChar));
            } else {
                snakeCaseString.append(aChar);
            }
        }
        return snakeCaseString.toString();
    }


    static StatusLine newStatusLine(RestStatus restStatus) {
        return new BasicStatusLine(HTTP_PROTOCOL, restStatus.getStatus(), restStatus.name());
    }
}
