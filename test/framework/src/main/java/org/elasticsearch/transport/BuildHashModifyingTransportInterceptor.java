/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.instanceOf;

class BuildHashModifyingTransportInterceptor implements TransportInterceptor {

    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        Executor executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler
    ) {

        if (TransportService.HANDSHAKE_ACTION_NAME.equals(action)) {
            return (request, channel, task) -> actualHandler.messageReceived(request, new TransportChannel() {
                @Override
                public String getProfileName() {
                    return channel.getProfileName();
                }

                @Override
                public void sendResponse(TransportResponse response) {
                    ESTestCase.assertThat(response, instanceOf(TransportService.HandshakeResponse.class));
                    final TransportService.HandshakeResponse handshakeResponse = (TransportService.HandshakeResponse) response;
                    channel.sendResponse(
                        new TransportService.HandshakeResponse(
                            handshakeResponse.getVersion(),
                            handshakeResponse.getBuildHash() + "-modified",
                            handshakeResponse.getDiscoveryNode(),
                            handshakeResponse.getClusterName()
                        )
                    );
                }

                @Override
                public void sendResponse(Exception exception) {
                    channel.sendResponse(exception);

                }
            }, task);
        } else {
            return actualHandler;
        }
    }
}
