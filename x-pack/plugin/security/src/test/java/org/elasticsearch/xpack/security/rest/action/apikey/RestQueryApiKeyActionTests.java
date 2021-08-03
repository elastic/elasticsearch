/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestQueryApiKeyActionTests extends ESTestCase {

    private final XPackLicenseState mockLicenseState = mock(XPackLicenseState.class);
    private Settings settings;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put("path.home", createTempDir().toString()).put("node.name", "test-" + getTestName())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        when(mockLicenseState.isSecurityEnabled()).thenReturn(true);
        threadPool = new ThreadPool(settings);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        final SearchModule searchModule = new SearchModule(Settings.EMPTY, false, org.elasticsearch.core.List.of());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testQueryParsing() throws Exception {
        final String query1 = "{\"query\":{\"bool\":{\"must\":[{\"terms\":{\"name\":[\"k1\",\"k2\"]}}]," +
            "\"should\":[{\"prefix\":{\"metadata.environ\":\"prod\"}}]}}}";
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray(query1), XContentType.JSON)
            .build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };

        try (NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                QueryApiKeyRequest queryApiKeyRequest = (QueryApiKeyRequest) request;
                final QueryBuilder queryBuilder = queryApiKeyRequest.getQueryBuilder();
                assertNotNull(queryBuilder);
                assertThat(queryBuilder.getClass(), is(BoolQueryBuilder.class));
                final BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
                assertTrue(boolQueryBuilder.filter().isEmpty());
                assertTrue(boolQueryBuilder.mustNot().isEmpty());
                assertThat(boolQueryBuilder.must().size(), equalTo(1));
                final QueryBuilder mustQueryBuilder = boolQueryBuilder.must().get(0);
                assertThat(mustQueryBuilder.getClass(), is(TermsQueryBuilder.class));
                assertThat(((TermsQueryBuilder) mustQueryBuilder).fieldName(), equalTo("name"));
                assertThat(boolQueryBuilder.should().size(), equalTo(1));
                final QueryBuilder shouldQueryBuilder = boolQueryBuilder.should().get(0);
                assertThat(shouldQueryBuilder.getClass(), is(PrefixQueryBuilder.class));
                assertThat(((PrefixQueryBuilder) shouldQueryBuilder).fieldName(), equalTo("metadata.environ"));
                listener.onResponse((Response) new QueryApiKeyResponse(org.elasticsearch.core.List.of()));
            }
        }) {
            final RestQueryApiKeyAction restQueryApiKeyAction = new RestQueryApiKeyAction(Settings.EMPTY, mockLicenseState);
            restQueryApiKeyAction.handleRequest(restRequest, restChannel, client);
        }

        assertNotNull(responseSetOnce.get());
    }
}
