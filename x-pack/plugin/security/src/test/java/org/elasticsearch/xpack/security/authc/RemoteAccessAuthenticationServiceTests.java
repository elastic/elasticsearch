/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteAccessAuthenticationServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private ApiKeyService apiKeyService;
    private AuthenticationService authenticationService;

    @Before
    public void init() throws Exception {
        this.apiKeyService = mock(ApiKeyService.class);
        this.authenticationService = mock(AuthenticationService.class);
        this.clusterService = mock(ClusterService.class);
    }

    public void testAuthenticateThrowsOnUnsupportedMinVersions() {
        clusterService = mockClusterServiceWithMinNodeVersion(VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_7_0));
        final var authcContext = mock(Authenticator.Context.class, Mockito.RETURNS_DEEP_STUBS);
        final var threadContext = new ThreadContext(Settings.EMPTY);
        when(authcContext.getThreadContext()).thenReturn(threadContext);
        when(authcContext.getRequest().exceptionProcessingRequest(any(), any())).thenAnswer(
            i -> new ElasticsearchSecurityException("potato", (Exception) i.getArguments()[0])
        );
        when(authenticationService.newContext(anyString(), any(), anyBoolean())).thenReturn(authcContext);
        final RemoteAccessAuthenticationService service = new RemoteAccessAuthenticationService(
            clusterService,
            apiKeyService,
            authenticationService
        );

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate("action", mock(TransportRequest.class), future);
        final ExecutionException actual = expectThrows(ExecutionException.class, future::get);

        assertThat(actual.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            actual.getCause().getCause().getMessage(),
            equalTo(
                "all nodes must have version ["
                    + Authentication.VERSION_REMOTE_ACCESS_REALM
                    + "] or higher to support cross cluster requests through the dedicated remote cluster port"
            )
        );
    }

    private ClusterService mockClusterServiceWithMinNodeVersion(final Version version) {
        final ClusterService clusterService = mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        when(clusterService.state().nodes().getMinNodeVersion()).thenReturn(version);
        return clusterService;
    }
}
