/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 *
 */
public class ClientTransportFilterTests extends ESTestCase {
    private AuthenticationService authcService;
    private ClientTransportFilter filter;

    @Before
    public void init() throws Exception {
        authcService = mock(AuthenticationService.class);
        filter = new ClientTransportFilter.Node(authcService);
    }

    public void testOutbound() throws Exception {
        TransportRequest request = mock(TransportRequest.class);
        filter.outbound("_action", request);
        verify(authcService).attachUserHeaderIfMissing(SystemUser.INSTANCE);
    }
}
