/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.junit.Before;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_TYPE_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetServiceAccountCredentialsActionTests extends ESTestCase {

    private TransportGetServiceAccountCredentialsAction transportGetServiceAccountCredentialsAction;
    private ServiceAccountService serviceAccountService;
    private Transport transport;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws UnknownHostException {
        final Settings.Builder builder = Settings.builder().put("node.name", "node_name").put("xpack.security.enabled", true);
        transport = mock(Transport.class);
        final TransportAddress transportAddress;
        if (randomBoolean()) {
            transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 9300);
        } else {
            transportAddress = new TransportAddress(InetAddress.getLocalHost(), 9300);
        }
        if (randomBoolean()) {
            builder.put("xpack.security.http.ssl.enabled", true);
        } else {
            builder.put(DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE);
        }
        when(transport.boundAddress()).thenReturn(new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress));
        final Settings settings = builder.build();
        serviceAccountService = mock(ServiceAccountService.class);
        transportGetServiceAccountCredentialsAction = new TransportGetServiceAccountCredentialsAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            serviceAccountService
        );
    }

    public void testDoExecuteWillDelegate() {
        final GetServiceAccountCredentialsRequest request = new GetServiceAccountCredentialsRequest(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8)
        );
        @SuppressWarnings("unchecked")
        final ActionListener<GetServiceAccountCredentialsResponse> listener = mock(ActionListener.class);
        transportGetServiceAccountCredentialsAction.doExecute(mock(Task.class), request, listener);
        verify(serviceAccountService).findTokensFor(eq(request), eq(listener));
    }
}
