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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Assert;

import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.arrayWithSize;
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
        GetResult result = new GetResult(SECURITY_MAIN_ALIAS, SINGLE_MAPPING_NAME, documentId, 0, 1, 1, true, source,
            emptyMap(), emptyMap());
        mockGetRequest(client, documentId, result);
    }

    public static void mockGetRequest(Client client, String documentId, GetResult result) {
        final GetRequestBuilder requestBuilder = new GetRequestBuilder(client, GetAction.INSTANCE);
        requestBuilder.setIndex(SECURITY_MAIN_ALIAS);
        requestBuilder.setType(SINGLE_MAPPING_NAME);
        requestBuilder.setId(documentId);
        when(client.prepareGet(SECURITY_MAIN_ALIAS, SINGLE_MAPPING_NAME, documentId)).thenReturn(requestBuilder);

        doAnswer(inv -> {
            Assert.assertThat(inv.getArguments(), arrayWithSize(2));
            Assert.assertThat(inv.getArguments()[0], instanceOf(GetRequest.class));
            final GetRequest request = (GetRequest) inv.getArguments()[0];
            Assert.assertThat(request.id(), equalTo(documentId));
            Assert.assertThat(request.index(), equalTo(SECURITY_MAIN_ALIAS));
            Assert.assertThat(request.type(), equalTo(SINGLE_MAPPING_NAME));

            Assert.assertThat(inv.getArguments()[1], instanceOf(ActionListener.class));
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) inv.getArguments()[1];
            listener.onResponse(new GetResponse(result));

            return null;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));
    }
}
