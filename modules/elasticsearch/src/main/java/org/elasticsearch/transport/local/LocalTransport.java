/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.io.ThrowableObjectInputStream;
import org.elasticsearch.util.io.stream.*;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.transport.BoundTransportAddress;
import org.elasticsearch.util.transport.LocalTransportAddress;
import org.elasticsearch.util.transport.TransportAddress;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.transport.Transport.Helper.*;
import static org.elasticsearch.util.concurrent.ConcurrentMaps.*;

/**
 * @author kimchy (shay.banon)
 */
public class LocalTransport extends AbstractLifecycleComponent<Transport> implements Transport {

    private final ThreadPool threadPool;

    private volatile TransportServiceAdapter transportServiceAdapter;

    private volatile BoundTransportAddress boundAddress;

    private volatile LocalTransportAddress localAddress;

    private final static ConcurrentMap<TransportAddress, LocalTransport> transports = newConcurrentMap();

    private static final AtomicLong transportAddressIdGenerator = new AtomicLong();

    private final ConcurrentMap<DiscoveryNode, LocalTransport> connectedNodes = newConcurrentMap();

    public LocalTransport(ThreadPool threadPool) {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS, threadPool);
    }

    @Inject public LocalTransport(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;
    }

    @Override public TransportAddress[] addressesFromString(String address) {
        return new TransportAddress[] {new LocalTransportAddress(address)};
    }

    @Override public boolean addressSupported(Class<? extends TransportAddress> address) {
        return LocalTransportAddress.class.equals(address);
    }

    @Override protected void doStart() throws ElasticSearchException {
        localAddress = new LocalTransportAddress(Long.toString(transportAddressIdGenerator.incrementAndGet()));
        transports.put(localAddress, this);
        boundAddress = new BoundTransportAddress(localAddress, localAddress);
    }

    @Override protected void doStop() throws ElasticSearchException {
        transports.remove(localAddress);
        // now, go over all the transports connected to me, and raise disconnected event
        for (LocalTransport targetTransport : transports.values()) {
            for (Map.Entry<DiscoveryNode, LocalTransport> entry : targetTransport.connectedNodes.entrySet()) {
                if (entry.getValue() == this) {
                    targetTransport.disconnectFromNode(entry.getKey());
                }
            }
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    @Override public void transportServiceAdapter(TransportServiceAdapter transportServiceAdapter) {
        this.transportServiceAdapter = transportServiceAdapter;
    }

    @Override public BoundTransportAddress boundAddress() {
        return boundAddress;
    }

    @Override public boolean nodeConnected(DiscoveryNode node) {
        return connectedNodes.containsKey(node);
    }

    @Override public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
        synchronized (this) {
            if (connectedNodes.containsKey(node)) {
                return;
            }
            final LocalTransport targetTransport = transports.get(node.address());
            if (targetTransport == null) {
                throw new ConnectTransportException(node, "Failed to connect");
            }
            connectedNodes.put(node, targetTransport);
            transportServiceAdapter.raiseNodeConnected(node);
        }
    }

    @Override public void disconnectFromNode(DiscoveryNode node) {
        synchronized (this) {
            LocalTransport removed = connectedNodes.remove(node);
            if (removed != null) {
                transportServiceAdapter.raiseNodeDisconnected(node);
            }
        }
    }

    @Override public <T extends Streamable> void sendRequest(final DiscoveryNode node, final long requestId, final String action,
                                                             final Streamable message, final TransportResponseHandler<T> handler) throws IOException, TransportException {
        HandlesStreamOutput stream = BytesStreamOutput.Cached.cachedHandles();

        stream.writeLong(requestId);
        byte status = 0;
        status = setRequest(status);
        stream.writeByte(status); // 0 for request, 1 for response.

        stream.writeUTF(action);
        message.writeTo(stream);

        final LocalTransport targetTransport = connectedNodes.get(node);
        if (targetTransport == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }

        final byte[] data = ((BytesStreamOutput) stream.wrappedOut()).copiedByteArray();
        threadPool.execute(new Runnable() {
            @Override public void run() {
                targetTransport.messageReceived(data, action, LocalTransport.this, handler);
            }
        });
    }

    ThreadPool threadPool() {
        return this.threadPool;
    }

    void messageReceived(byte[] data, String action, LocalTransport sourceTransport, @Nullable final TransportResponseHandler responseHandler) {
        StreamInput stream = new BytesStreamInput(data);
        stream = HandlesStreamInput.Cached.cached(stream);

        try {
            long requestId = stream.readLong();
            byte status = stream.readByte();
            boolean isRequest = isRequest(status);

            if (isRequest) {
                handleRequest(stream, requestId, sourceTransport);
            } else {
                final TransportResponseHandler handler = transportServiceAdapter.remove(requestId);
                if (handler == null) {
                    throw new ResponseHandlerNotFoundTransportException(requestId);
                }
                if (Transport.Helper.isError(status)) {
                    handlerResponseError(stream, handler);
                } else {
                    handleResponse(stream, handler);
                }
            }
        } catch (Exception e) {
            if (responseHandler != null) {
                responseHandler.handleException(new RemoteTransportException(nodeName(), localAddress, action, e));
            } else {
                logger.warn("Failed to receive message for action [" + action + "]", e);
            }
        }
    }

    private void handleRequest(StreamInput stream, long requestId, LocalTransport sourceTransport) throws Exception {
        final String action = stream.readUTF();
        final LocalTransportChannel transportChannel = new LocalTransportChannel(this, sourceTransport, action, requestId);
        try {
            final TransportRequestHandler handler = transportServiceAdapter.handler(action);
            if (handler == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final Streamable streamable = handler.newInstance();
            streamable.readFrom(stream);
            handler.messageReceived(streamable, transportChannel);
        } catch (Exception e) {
            try {
                transportChannel.sendResponse(e);
            } catch (IOException e1) {
                logger.warn("Failed to send error message back to client for action [" + action + "]", e);
                logger.warn("Actual Exception", e1);
            }
        }
    }


    private void handleResponse(StreamInput buffer, final TransportResponseHandler handler) {
        final Streamable streamable = handler.newInstance();
        try {
            streamable.readFrom(buffer);
        } catch (Exception e) {
            handleException(handler, new TransportSerializationException("Failed to deserialize response of type [" + streamable.getClass().getName() + "]", e));
            return;
        }
        if (handler.spawn()) {
            threadPool.execute(new Runnable() {
                @SuppressWarnings({"unchecked"}) @Override public void run() {
                    try {
                        handler.handleResponse(streamable);
                    } catch (Exception e) {
                        handleException(handler, new ResponseHandlerFailureTransportException("Failed to handler response", e));
                    }
                }
            });
        } else {
            try {
                //noinspection unchecked
                handler.handleResponse(streamable);
            } catch (Exception e) {
                handleException(handler, new ResponseHandlerFailureTransportException("Failed to handler response", e));
            }
        }
    }

    private void handlerResponseError(StreamInput buffer, final TransportResponseHandler handler) {
        Throwable error;
        try {
            ThrowableObjectInputStream ois = new ThrowableObjectInputStream(buffer);
            error = (Throwable) ois.readObject();
        } catch (Exception e) {
            error = new TransportSerializationException("Failed to deserialize exception response from stream", e);
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException("None remote transport exception", error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        handler.handleException(rtx);
    }
}
