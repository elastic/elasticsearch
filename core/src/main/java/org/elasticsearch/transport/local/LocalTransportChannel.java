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

package org.elasticsearch.transport.local;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.support.TransportStatus;

import java.io.IOException;

/**
 *
 */
public class LocalTransportChannel implements TransportChannel {

    private static final String LOCAL_TRANSPORT_PROFILE = "default";

    private final LocalTransport sourceTransport;
    private final TransportServiceAdapter sourceTransportServiceAdapter;
    // the transport we will *send to*
    private final LocalTransport targetTransport;
    private final String action;
    private final long requestId;
    private final Version version;

    public LocalTransportChannel(LocalTransport sourceTransport, TransportServiceAdapter sourceTransportServiceAdapter, LocalTransport targetTransport, String action, long requestId, Version version) {
        this.sourceTransport = sourceTransport;
        this.sourceTransportServiceAdapter = sourceTransportServiceAdapter;
        this.targetTransport = targetTransport;
        this.action = action;
        this.requestId = requestId;
        this.version = version;
    }

    @Override
    public String action() {
        return action;
    }

    @Override
    public String getProfileName() {
        return LOCAL_TRANSPORT_PROFILE;
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        sendResponse(response, TransportResponseOptions.EMPTY);
    }

    @Override
    public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
        try (BytesStreamOutput stream = new BytesStreamOutput()) {
            stream.setVersion(version);
            stream.writeLong(requestId);
            byte status = 0;
            status = TransportStatus.setResponse(status);
            stream.writeByte(status); // 0 for request, 1 for response.
            response.writeTo(stream);
            final byte[] data = stream.bytes().toBytes();
            targetTransport.workers().execute(new Runnable() {
                @Override
                public void run() {
                    targetTransport.messageReceived(data, action, sourceTransport, version, null);
                }
            });
            sourceTransportServiceAdapter.onResponseSent(requestId, action, response, options);
        }
    }

    @Override
    public void sendResponse(Throwable error) throws IOException {
        BytesStreamOutput stream = new BytesStreamOutput();
        writeResponseExceptionHeader(stream);
        RemoteTransportException tx = new RemoteTransportException(targetTransport.nodeName(), targetTransport.boundAddress().boundAddress(), action, error);
        stream.writeThrowable(tx);

        final byte[] data = stream.bytes().toBytes();
        targetTransport.workers().execute(new Runnable() {
            @Override
            public void run() {
                targetTransport.messageReceived(data, action, sourceTransport, version, null);
            }
        });
        sourceTransportServiceAdapter.onResponseSent(requestId, action, error);
    }

    private void writeResponseExceptionHeader(BytesStreamOutput stream) throws IOException {
        stream.writeLong(requestId);
        byte status = 0;
        status = TransportStatus.setResponse(status);
        status = TransportStatus.setError(status);
        stream.writeByte(status);
    }
}
