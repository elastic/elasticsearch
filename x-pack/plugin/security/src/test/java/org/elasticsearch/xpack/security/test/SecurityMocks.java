/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.test;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.mockito.Mockito;

import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_INDEX_NAME;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utility class for constructing commonly used mock objects.
 * <em>Note to maintainers</em>:
 * It is not intended that this class cover _all_ mocking scenarios. Consider very carefully before adding methods to this class that are
 * only used in one or 2 places. This class is intended for the situations where a common piece of complex mock code is used in multiple
 * test suites.
 */
public final class SecurityMocks {

    private static final String DOC_TYPE = "doc";

    private SecurityMocks() {
        throw new IllegalStateException("Cannot instantiate utility class");
    }

    public static SecurityIndexManager mockSecurityIndexManager() {
        return mockSecurityIndexManager(true, true);
    }

    public static SecurityIndexManager mockSecurityIndexManager(boolean exists, boolean available) {
        final SecurityIndexManager securityIndexManager = mock(SecurityIndexManager.class);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndexManager).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndexManager).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
        when(securityIndexManager.indexExists()).thenReturn(exists);
        when(securityIndexManager.isAvailable()).thenReturn(available);
        return securityIndexManager;
    }

    public static void mockGetRequest(Client client, String documentId, BytesReference source) {
        GetResult result = new GetResult(SECURITY_INDEX_NAME, DOC_TYPE, documentId, 0, 1, 1, true, source, emptyMap());
        mockGetRequest(client, documentId, result);
    }

    public static void mockGetRequest(Client client, String documentId, GetResult result) {
        final GetRequestBuilder requestBuilder = new GetRequestBuilder(client, GetAction.INSTANCE);
        requestBuilder.setIndex(SECURITY_INDEX_NAME);
        requestBuilder.setType(DOC_TYPE);
        requestBuilder.setId(documentId);
        when(client.prepareGet(SECURITY_INDEX_NAME, DOC_TYPE, documentId)).thenReturn(requestBuilder);

        doAnswer(inv -> {
            Assert.assertThat(inv.getArguments(), arrayWithSize(2));
            Assert.assertThat(inv.getArguments()[0], instanceOf(GetRequest.class));
            final GetRequest request = (GetRequest) inv.getArguments()[0];
            Assert.assertThat(request.id(), equalTo(documentId));
            Assert.assertThat(request.index(), equalTo(SECURITY_INDEX_NAME));
            Assert.assertThat(request.type(), equalTo(DOC_TYPE));

            Assert.assertThat(inv.getArguments()[1], instanceOf(ActionListener.class));
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) inv.getArguments()[1];
            listener.onResponse(new GetResponse(result));

            return null;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));
    }

    public static void mockSearchHits(Client client, SearchHit[] hits) {
        mockSearchHits(client, ".security", null, hits);
    }

    public static void mockSearchHits(Client client, String index, @Nullable String type, SearchHit[] hits) {
        Mockito.doAnswer(invocationOnMock -> {
            Assert.assertThat(invocationOnMock.getArguments()[1], Matchers.instanceOf(SearchRequest.class));
            SearchRequest request = (SearchRequest) invocationOnMock.getArguments()[1];
            Assert.assertThat(request.indices(), arrayContaining(index));
            Assert.assertThat(request.types(), type == null ? emptyArray() : arrayContaining(type));

            Assert.assertThat(invocationOnMock.getArguments()[2], Matchers.instanceOf(ActionListener.class));
            ActionListener<SearchResponse> listener = (ActionListener) invocationOnMock.getArguments()[2];
            final SearchResponseSections inner = new SearchResponseSections(new SearchHits(hits, hits.length, 0.0f),
                null, null, false, false, null, 0);
            listener.onResponse(new SearchResponse(inner, null, 0, 0, 0, randomLongBetween(1, 500), new ShardSearchFailure[0], null));
            return null;
        }).when(client).execute(Mockito.same(SearchAction.INSTANCE), any(SearchRequest.class), any(ActionListener.class));
    }
}
