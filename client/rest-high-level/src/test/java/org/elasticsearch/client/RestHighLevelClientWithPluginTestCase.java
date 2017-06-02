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
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.client.ESRestHighLevelClientTestCase.execute;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Test usage of search extensions provided by plugins with the {@link RestHighLevelClient}.
 */
public abstract class RestHighLevelClientWithPluginTestCase extends ESTestCase {

    private static final String CUSTOM = "custom";

    private RestClient restClient;
    private RestHighLevelClient restHighLevelClient;

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    @Before
    public void iniClients() throws IOException {
        if (restHighLevelClient == null) {
            restClient = mock(RestClient.class);
            restHighLevelClient = new RestHighLevelClient(restClient, getPlugins());

            doAnswer(mock -> performRequest((HttpEntity) mock.getArguments()[3]))
                .when(restClient)
                    .performRequest(eq(HttpGet.METHOD_NAME), eq("/_search"), anyMapOf(String.class, String.class),
                            anyObject(), anyVararg());
            doAnswer(mock -> performRequestAsync((HttpEntity) mock.getArguments()[3], (ResponseListener) mock.getArguments()[4]))
                .when(restClient)
                    .performRequestAsync(eq(HttpGet.METHOD_NAME), eq("/_search"), anyMapOf(String.class, String.class),
                            any(HttpEntity.class), any(ResponseListener.class), anyVararg());
        }
    }

    protected abstract Response performRequest(HttpEntity httpEntity) throws IOException;

    protected Void performRequestAsync(HttpEntity httpEntity, ResponseListener responseListener) {
        try {
            responseListener.onSuccess(performRequest(httpEntity));
        } catch (IOException e) {
            responseListener.onFailure(e);
        }
        return null;
    }

    protected SearchResponse search(SearchRequest searchRequest) throws IOException {
        return execute(searchRequest, restHighLevelClient::search, restHighLevelClient::searchAsync);
    }

    /**
     * Creates a {@link Response} from a {@link SearchResponse}?
     */
    protected Response createResponse(SearchResponse searchResponse) throws IOException {
        ProtocolVersion protocol = new ProtocolVersion("HTTP", 1, 1);
        HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocol, 200, "OK"));

        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesRef bytesRef = XContentHelper.toXContent(searchResponse, XContentType.JSON, params, false).toBytesRef();
        httpResponse.setEntity(new ByteArrayEntity(bytesRef.bytes, ContentType.APPLICATION_JSON));

        RequestLine requestLine = new BasicRequestLine(HttpGet.METHOD_NAME, "/_search", protocol);
        return new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
    }
}
