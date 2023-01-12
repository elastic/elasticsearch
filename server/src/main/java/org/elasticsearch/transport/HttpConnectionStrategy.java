/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.stream.Stream;

public class HttpConnectionStrategy extends RemoteConnectionStrategy {

    /**
     * The remote address for the proxy. The connections will be opened to the configured address.
     */
    public static final Setting.AffixSetting<String> HTTP_ADDRESS = Setting.affixKeySetting(
        "cluster.remote.",
        "http_address",
        (ns, key) -> Setting.simpleString(key, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<String> HTTP_AUTHORIZATION = Setting.affixKeySetting(
        "cluster.remote.",
        "http_authorization",
        (ns, key) -> Setting.simpleString(key, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    static final int CHANNELS_PER_CONNECTION = 0;

    private final String configuredAddress;
    private final Transport.Connection connection;
    private final String httpAuthorization;
    private TransportRequestRelay transportRequestRelay;

    HttpConnectionStrategy(
        String clusterAlias,
        TransportService transportService,
        RemoteConnectionManager connectionManager,
        Settings settings
    ) {
        super(clusterAlias, transportService, connectionManager, settings);
        this.configuredAddress = HTTP_ADDRESS.getConcreteSettingForNamespace(clusterAlias).get(settings);
        this.httpAuthorization = HTTP_AUTHORIZATION.getConcreteSettingForNamespace(clusterAlias).get(settings);
        connection = new StubConnection(this);
    }

    public String getClusterAlias() {
        return clusterAlias;
    }

    public String getConfiguredAddress() {
        return configuredAddress;
    }

    public String getHttpAuthorization() {
        return httpAuthorization;
    }

    public TransportRequestRelay getTransportRequestRelay() {
        return transportRequestRelay;
    }

    public synchronized void setTransportRequestRelay(TransportRequestRelay transportRequestRelay) {
        if (this.transportRequestRelay == null) {
            this.transportRequestRelay = transportRequestRelay;
            this.transportRequestRelay.start();
        }
    }

    static Stream<Setting.AffixSetting<?>> enablementSettings() {
        return Stream.of(HttpConnectionStrategy.HTTP_ADDRESS, HttpConnectionStrategy.HTTP_AUTHORIZATION);
    }

    static Writeable.Reader<RemoteConnectionInfo.ModeInfo> infoReader() {
        return HttpModeInfo::new;
    }

    @Override
    protected boolean shouldOpenMoreConnections() {
        return false;
    }

    @Override
    protected boolean strategyMustBeRebuilt(Settings newSettings) {
        return false;
    }

    @Override
    protected ConnectionStrategy strategyType() {
        return ConnectionStrategy.HTTP;
    }

    @Override
    protected void connectImpl(ActionListener<Void> listener) {
        listener.onResponse(null);
    }

    @Override
    public RemoteConnectionInfo.ModeInfo getModeInfo() {
        return new HttpModeInfo(configuredAddress);
    }

    public Transport.Connection getConnection() {
        return connection;
    }

    @Override
    public void close() {
        super.close();
        if (transportRequestRelay != null) {
            try {
                transportRequestRelay.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static class HttpModeInfo implements RemoteConnectionInfo.ModeInfo {

        private final String address;

        public HttpModeInfo(String address) {
            this.address = address;
        }

        private HttpModeInfo(StreamInput input) throws IOException {
            address = input.readString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("proxy_address", address);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(address);
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public String modeName() {
            return "http";
        }

        public String getAddress() {
            return address;
        }

        @Override
        public ConnectionStrategy modeType() {
            return ConnectionStrategy.PROXY;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HttpModeInfo otherProxy = (HttpModeInfo) o;
            return Objects.equals(address, otherProxy.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address);
        }
    }

    public static class StubConnection implements Transport.Connection {

        private final HttpConnectionStrategy httpConnectionStrategy;

        public StubConnection(HttpConnectionStrategy httpConnectionStrategy) {
            this.httpConnectionStrategy = httpConnectionStrategy;
        }

        public HttpConnectionStrategy getHttpConnectionStrategy() {
            return httpConnectionStrategy;
        }

        @Override
        public void incRef() {

        }

        @Override
        public boolean tryIncRef() {
            return false;
        }

        @Override
        public boolean decRef() {
            return false;
        }

        @Override
        public boolean hasReferences() {
            return false;
        }

        @Override
        public DiscoveryNode getNode() {
            return httpConnectionStrategy.transportService.getLocalNode();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {

        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {

        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {

        }

        @Override
        public void onRemoved() {

        }

        @Override
        public void addRemovedListener(ActionListener<Void> listener) {

        }
    }

    public interface TransportRequestRelay {
        void relayRequest(String clusterAlias, String action, TransportRequest transportRequest, ActionListener<byte[]> listener);

        void start();

        void close() throws IOException;
    }

}
